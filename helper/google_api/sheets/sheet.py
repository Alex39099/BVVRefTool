from __future__ import annotations

import copy
from typing import TYPE_CHECKING, Any

from helper.google_api.sheets._tracked_model import TrackedModel
from helper.google_api.sheets.grid_range import GridRange
from helper.google_api.sheets.value_range import (
    FetchedRange,
    ValueRange,
    ValueRenderOption,
)

if TYPE_CHECKING:
    from helper.google_api.sheets.data_validation import DataValidation
    from helper.google_api.sheets.developer_metadata import SheetDeveloperMetadata
    from helper.google_api.sheets.protected_range import ProtectedRange
    from helper.google_api.sheets.spreadsheet import Spreadsheet
    from helper.google_api.sheets.value_range import CellValue


class Sheet(TrackedModel):
    def __init__(self, spreadsheet: Spreadsheet, propertiesjson: dict[str, Any], fetched_values: FetchedRange | None = None, is_dirty: bool = False, from_duplicate: bool = False):
        """ Internal. Use methods of Spreadsheet instead. """
        super().__init__(is_dirty)
        self.spreadsheet = spreadsheet
        self._propertiesjson: dict[str, Any] = copy.deepcopy(propertiesjson)
        self.stale: bool = False
        self._initial_title: str = self.title
        self._initial_row_count: int = self.row_count
        self._initial_column_count: int = self.column_count
        self.fetched_values: FetchedRange | None = fetched_values
        self._from_duplicate: bool = False
        
    def _duplicate(self, new_sheet_id: int, new_sheet_title: str) -> Sheet:
        if not isinstance(new_sheet_id, int):
            raise TypeError("new_sheet_id must be of type int")
        if not isinstance(new_sheet_title, str) or not new_sheet_title:
            raise ValueError("new_sheet_title must be a non empty str")
        self.stale = True
        new_sheet = Sheet(
            spreadsheet=self.spreadsheet,
            propertiesjson=self.properties_json,
            fetched_values=self.fetched_values,
            is_dirty=True,
            from_duplicate=True
        )
        new_sheet._set_id(new_sheet_id)
        new_sheet.title = new_sheet_title
        if new_sheet.fetched_values is not None:
            new_sheet.fetched_values.mark_clean()
        return new_sheet
    
    def duplicate(self, new_sheet_id: int | None, new_sheet_title: str | None) -> Sheet:
        """ Duplicates this sheet within the spreadsheet

        Args:
            new_sheet_id (int | None): id of the new sheet. Defaults to some random id.
            new_sheet_title (str | None): title of the new sheet. Defaults to "Copy of self.title".

        Returns:
            Sheet: the duplicated sheet
        """
        return self.spreadsheet.duplicate_sheet(
            source_sheet_id=self.id, 
            new_sheet_id=new_sheet_id, 
            new_sheet_title=new_sheet_title
        )
        
    def fetch_values(self, overwrite_local_changes: bool = True, value_render_option: ValueRenderOption = ValueRenderOption.FORMULA):
        """ Fetches all values of the spreadsheet.

        Args:
            overwrite_local_changes (bool, optional): _description_. Defaults to True.
            value_render_option (ValueRenderOption): value render option for the fetch. Defaults to ValueRenderOption.FORMULA.

        Raises:
            ValueError: if there are local value changes and overwrite_local_changes is False
        """
        if not overwrite_local_changes and self.is_value_dirty:
            raise ValueError("There are local value changes and overwrite_local_changes is False")
        raw = self.spreadsheet._gspreadsheets_client.values().get(
            spreadsheetId=self.spreadsheet.id,
            range=self.grid_range.to_a1_notation(self._initial_title),
            valueRenderOption=value_render_option
        ).execute()
        value_range = ValueRange.from_json(raw, value_render_option=value_render_option)
        self.fetched_values = FetchedRange.from_value_range(sheet=self, value_range=value_range)
        
    def __getitem__(self, key: tuple[int, int] | str) -> CellValue:
        if self.fetched_values is None:
            self.fetch_values()
        assert self.fetched_values is not None
        return self.fetched_values[key]

    def __setitem__(self, key: tuple[int, int] | str, value: CellValue) -> None:
        self.raise_for_stale()
        if self.fetched_values is None:
            self.fetch_values()
        assert self.fetched_values is not None
        self.fetched_values[key] = value

    def __delitem__(self, key: tuple[int, int] | str) -> None:
        self.raise_for_stale()
        if self.fetched_values is None:
            self.fetch_values()
        assert self.fetched_values is not None
        del self.fetched_values[key]
    
    def raise_for_stale(self):
        if self.stale:
            raise ValueError("instance is stale. Sync first.")
        
    def mark_clean(self) -> None:
        super().mark_clean()
        self.stale = False
        self._initial_title = self.title
        self._initial_row_count = self.row_count
        self._initial_column_count = self.column_count
        self._from_duplicate = False
        
    @property
    def is_value_dirty(self) -> bool:
        return self.fetched_values is not None and self.fetched_values.is_dirty
    
    def mark_value_clean(self) -> None:
        if self.fetched_values is not None:
            self.fetched_values.mark_clean()
            
    @property
    def properties_json(self) -> dict[str, Any]:
        return copy.deepcopy(self._propertiesjson)

    @property
    def id(self) -> int:
        return self._propertiesjson["sheetId"]
    
    def _set_id(self, value: int) -> None:
        self._propertiesjson["sheetId"] = value
    
    @property
    def title(self) -> str:
        return self._propertiesjson["title"]
    
    @title.setter
    def title(self, value: str) -> None:
        if not isinstance(value, str) or not value:
            raise ValueError("title must be a non-empty string")
        for sheet in self.spreadsheet.sheets:
            if sheet.title == value and sheet != self:
                raise ValueError("title already present in spreadsheet")
        self._propertiesjson["title"] = value
        self._mark_dirty('properties.title')
        
    @property
    def initial_row_count(self) -> int:
        return self._initial_row_count
    
    @property
    def initial_column_count(self) -> int:
        return self._initial_column_count
        
    @property
    def row_count(self) -> int:
        return self._propertiesjson["gridProperties"]["rowCount"]
    
    @row_count.setter
    def row_count(self, value: int) -> None:
        self.raise_for_stale()
        if not isinstance(value, int):
            raise TypeError("row_count must be an integer")
        if self._from_duplicate and value < self.row_count:
            raise ValueError("cannot downscale duplicated sheet. Sync with Cloud first.")
        self._propertiesjson["gridProperties"]["rowCount"] = value
        self._mark_dirty("properties.gridProperties.rowCount")
        self._resize_fetched_range()
    
    @property
    def column_count(self) -> int:
        return self._propertiesjson["gridProperties"]["columnCount"]
    
    @column_count.setter
    def column_count(self, value: int) -> None:
        self.raise_for_stale()
        if not isinstance(value, int):
            raise TypeError("column_count must be an integer")
        if self._from_duplicate and value < self.column_count:
            raise ValueError("cannot downscale duplicated sheet. Sync with Cloud first.")
        self._propertiesjson["gridProperties"]["columnCount"] = value
        self._mark_dirty("properties.gridProperties.columnCount")
        self._resize_fetched_range()
        
    def _resize_fetched_range(self) -> None:
        if self.fetched_values is None:
            return
        self.fetched_values.apply_sheet_bounds()
        
    @property
    def grid_range(self):
        return GridRange(
            sheet_id=self.id,
            start_row_idx=0,
            end_row_idx=self.row_count - 1,
            start_column_idx=0,
            end_column_idx=self.column_count - 1
        )
    
    @property
    def frozen_row_count(self) -> int:
        return self._propertiesjson["gridProperties"].get('frozenRowCount', 0)
    
    @frozen_row_count.setter
    def frozen_row_count(self, value: int) -> None:
        self.raise_for_stale()
        if not isinstance(value, int):
            raise TypeError("frozen_row_count must be an integer")
        self._propertiesjson["gridProperties"]["frozenRowCount"] = value
        self._mark_dirty("properties.gridProperties.frozenRowCount")
        
    @property
    def frozen_column_count(self) -> int:
        return self._propertiesjson["gridProperties"].get('frozenColumnCount', 0)

    @frozen_column_count.setter
    def frozen_column_count(self, value: int) -> None:
        self.raise_for_stale()
        if not isinstance(value, int):
            raise TypeError("frozen_column_count must be an integer")
        self._propertiesjson["gridProperties"]["frozenColumnCount"] = value
        self._mark_dirty("properties.gridProperties.frozenColumnCount")
    
    def apply_data_validation(self, validation: DataValidation):
        if validation.sheet != self:
            raise ValueError("dataValidation does not belong to this sheet")
        self.spreadsheet.apply_data_validation(validation)
    
    @property
    def protected_ranges(self) -> tuple[ProtectedRange, ...]:
        return self.spreadsheet.protected_ranges[self.id]
    
    def add_protected_range(self, protected_range: ProtectedRange):
        """ Adds a protected range to this sheet.

        Args:
            protected_range (ProtectedRange): the protected range

        Raises:
            ValueError: if the protected range does not belong to this sheet.
        """
        if self.id != protected_range.range.sheet_id:
            raise ValueError(f"sheet_id do not match: self = {self.id} vs. protectedRange = {protected_range.range.sheet_id}")
        self.spreadsheet.add_protected_range(protected_range)
    
    def remove_protected_range(self, protected_range: ProtectedRange):
        """ Removes a protected range from this sheet.

        Args:
            protected_range (ProtectedRange): the protected range

        Raises:
            ValueError: if the protected range does not belong to this sheet or was not synced to cloud yet.
        """
        if self.id != protected_range.range.sheet_id:
            raise ValueError(f"sheet_id do not match: self = {self.id} vs. protectedRange = {protected_range.range.sheet_id}")
        if protected_range.id is None:
            raise ValueError("cannot remove protected range without id (not synced yet)")
        self.spreadsheet.remove_protected_range(protected_range_id=protected_range.id)
        
    @property
    def developer_metadata(self) -> SheetDeveloperMetadata:
        return self.spreadsheet._developerMetadata[self.id]
    
    def copy_paste(self, source: GridRange, destinations: list[GridRange], value_render_option: ValueRenderOption | None = ValueRenderOption.FORMULA, skip_fetch: bool = False):
        """ Immediately copy&pastes a source range to (multiple) destinations. Fetches values afterwards.

        Args:
            source (GridRange): source to copy from.
            destinations (list[GridRange]): list of destinations to copy source to (must not overlap with source).
            value_render_option (ValueRenderOption): value render option for the subsequent fetch. Defaults to ValueRenderOption.FORMULA.
            skip_fetch (bool): If true, skips the subsequent fetch and instead resets fetched_values of this sheet. Defaults to False.

        Raises:
            ValueError: sheet is stale or was recently duplicated, fetched values are dirty, gridRanges outside the sheet or if source and destinations overlap.
        """
        self.raise_for_stale()
        if not skip_fetch and not isinstance(value_render_option, ValueRenderOption):
            raise ValueError("value_render_option must be specified for skip_fetch == False")
        
        if self._from_duplicate:
            raise ValueError("sheet is not yet synced with cloud")
        if self.is_value_dirty:
            raise ValueError("cannot copy&paste if values are dirty. Sync to cloud first.")
        if any(grid_range.sheet_id != self.id for grid_range in [source] + destinations):
            raise ValueError("can only copy within this sheet")
        if any(source.overlaps(destination) for destination in destinations):
            raise ValueError("destinations must not overlap with source")
        self.spreadsheet._copy_paste(source, destinations)
        if not skip_fetch:
            assert isinstance(value_render_option, ValueRenderOption)
            self.fetch_values(value_render_option=value_render_option)
