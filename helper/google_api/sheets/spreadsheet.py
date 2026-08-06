from __future__ import annotations

import random
from collections.abc import Iterator
from itertools import chain
from typing import TYPE_CHECKING, Any

from google.auth.credentials import Credentials as BaseCredentials
from googleapiclient.discovery import build

from helper.google_api.sheets.data_validation import DataValidation
from helper.google_api.sheets.developer_metadata import SheetDeveloperMetadata
from helper.google_api.sheets.protected_range import ProtectedRange
from helper.google_api.sheets.sheet import Sheet

if TYPE_CHECKING:
    from helper.google_api.sheets.grid_range import GridRange
    from helper.google_api.sheets.value_range import ValueRange


class Spreadsheet:
    
    _id: str
    _url: str
    _propertiesjson: dict[str, Any]
    
    _sheets: list[Sheet]
    _original_sheet_order: list[int]
    _duplicate_sheet_requests: list[dict[str, Any]]
    _removing_sheet_ids: set[int]
    
    _data_validations: dict[int, list[DataValidation]]
    
    _protected_ranges: dict[int, set[ProtectedRange]]
    _removing_protected_range_ids: dict[int, set[int]]
    
    _developerMetadata: dict[int, SheetDeveloperMetadata]
    
    def __init__(self, gspreadsheets_client, spreadsheet_id: str) -> None:
        """ Internal only. Use Spreadsheet.from_cloud. """
        self._gspreadsheets_client = gspreadsheets_client
        fields = "spreadsheetId,spreadsheetUrl,properties,sheets.properties,sheets.protectedRanges,sheets.developerMetadata"
        spreadsheet_json: dict[str, Any] = self._gspreadsheets_client.get(
            spreadsheetId=spreadsheet_id,
            fields=fields
        ).execute()
        
        self._id: str = spreadsheet_json['spreadsheetId']
        self._url: str = spreadsheet_json['spreadsheetUrl']
        self._propertiesjson: dict[str, Any] = spreadsheet_json['properties']
        
        self._sheets: list[Sheet] = []
        self._removing_sheet_ids: set[int] = set()
        self._protected_ranges: dict[int, set[ProtectedRange]] = {}
        self._removing_protected_range_ids: dict[int, set[int]] = {}
        self._developerMetadata: dict[int, SheetDeveloperMetadata] = {}
        for sheet_json in spreadsheet_json['sheets']:
            sheet = Sheet(spreadsheet=self, propertiesjson=sheet_json['properties'])
            self._sheets.append(sheet)
            sheet_id = sheet.id
            self._data_validations[sheet_id] = []
            self._protected_ranges[sheet_id] = {ProtectedRange.from_json(d) for d in sheet_json.get('protectedRanges', [])}
            self._removing_protected_range_ids[sheet_id] = set()
            sheet_developer_metadata_json = sheet_json.get('developerMetadata', [{}])[0]
            self._developerMetadata[sheet_id] = (
                SheetDeveloperMetadata.from_json(sheet_developer_metadata_json) 
                if sheet_developer_metadata_json and 
                sheet_developer_metadata_json['metadataKey'] == SheetDeveloperMetadata.METADATA_KEY 
                else SheetDeveloperMetadata(sheet_id)
            )

        self._original_sheet_order: list[int] = [s.id for s in self._sheets]
        self._duplicate_sheet_requests: list[dict[str, Any]] = []
        
    # =========================================================================================================
    
    @property
    def id(self) -> str:
        return self._id
    
    @property
    def title(self) -> str:
        return self._propertiesjson["title"]
    
    @property
    def url(self) -> str:
        return self._url
    
    # =========================================================================================================
        
    @property
    def sheets(self) -> tuple[Sheet, ...]:
        return tuple(self._sheets)
    
    def get_sheet_by_id(self, sheet_id: int) -> Sheet:
        """ Get a sheet by its id.

        Args:
            sheet_id (int): id of sheet

        Raises:
            IndexError: if there is no sheet with the given id

        Returns:
            Sheet: the sheet with the given id
        """
        for sheet in self._sheets:
            if sheet.id == sheet_id:
                return sheet
        raise IndexError(f"no sheet with id {sheet_id}")
    
    def get_sheet_by_title(self, sheet_title: str) -> Sheet:
        """ Get a sheet by its title.

        Args:
            sheet_title (str): title of sheet
        Raises:
            IndexError: if there is no sheet with the given title

        Returns:
            Sheet: the sheet with the given title
        """
        for sheet in self._sheets:
            if sheet.title == sheet_title:
                return sheet
        raise IndexError(f"no sheet with title {sheet_title}")
    
    def reorder_sheets(self, sheet_ids: list[int]) -> None:
        """ Reorders the current sheets by id

        Args:
            sheet_ids (list[int]): sheet ids in order

        Raises:
            ValueError: if sheet ids are missing
        """
        current_ids = [s.id for s in self._sheets]
        if sorted(sheet_ids) != sorted(current_ids):
            raise ValueError("sheet_ids must contain exactly the same ids as the current sheets")
        self._sheets = [self.get_sheet_by_id(sid) for sid in sheet_ids]
        
    def duplicate_sheet(self, 
                        source_sheet_id: int, 
                        new_sheet_id: int | None, 
                        new_sheet_title: str | None,
                        copy_protected_ranges: bool = True,
                        copy_developer_metadata: bool = True) -> Sheet:
        """ Duplicates a sheet within the spreadsheet. The source_sheet will be stale afterwards.

        Args:
            source_sheet_id (int): id of the source sheet
            new_sheet_id (int | None): id of the new sheet, optional
            new_sheet_title (str | None): title of the new sheet, defaults to "Copy of source.title
            copy_protected_ranges (bool, optional): if true, includes protected ranges. Defaults to True.
            copy_developer_metadata (bool, optional): if true, includes developerMetadata. Defaults to True.

        Returns:
            Sheet: the new sheet, added to the spreadsheet (but not yet synced)
        """
        source_sheet = self.get_sheet_by_id(source_sheet_id)
        if new_sheet_id is None:
            # choose a random sheet_id
            excluded = {sheet.id for sheet in self.sheets}
            new_sheet_id = random.choice(list(set(range(10_000)) - excluded))
        if new_sheet_title is None:
            new_sheet_title = f"Copy of {source_sheet.title}"
        new_sheet = source_sheet._duplicate(new_sheet_id=new_sheet_id, new_sheet_title=new_sheet_title)
        self._sheets.append(new_sheet)
        self._duplicate_sheet_requests.append({
            "sourceSheetId": source_sheet_id,
            "insertSheetIndex": 0,  # sheets get resorted anyway
            "newSheetId": new_sheet_id,
            "newSheetName": new_sheet_title
        })
        if copy_protected_ranges:
            protected_ranges = {pr._copy_to(new_sheet_id) for pr in self._protected_ranges[source_sheet_id]}
            self._protected_ranges[new_sheet_id] = protected_ranges
        else:
            self._protected_ranges[new_sheet_id] = set()
        if copy_developer_metadata:
            self._developerMetadata[new_sheet_id] = self._developerMetadata[source_sheet_id]._copy_to(sheet_id=new_sheet_id)
        else:
            self._developerMetadata[new_sheet_id] = SheetDeveloperMetadata(sheet_id=new_sheet_id)
        return new_sheet
        
    def remove_sheet(self, sheet_id: int):
        """ Removes a sheet from the spreadsheet.

        Args:
            sheet_id (int): id of the sheet
        """
        sheet = self.get_sheet_by_id(sheet_id)
        sheet.raise_for_stale()
        self._original_sheet_order.remove(sheet_id)
        self._sheets.remove(sheet)
        del self._protected_ranges[sheet_id]
        self._removing_sheet_ids.add(sheet_id)
        
    # =========================================================================================================
    
    def apply_data_validation(self, validation: DataValidation):
        if validation.sheet not in self.sheets:
            raise ValueError("validation range is not part of this spreadsheet")
        if validation.rule is not None and validation.rule.sheet not in self.sheets:
            raise ValueError("validation rule is outside this spreadsheet")
        self._data_validations[validation.sheet.id].append(validation)
    
    # =========================================================================================================
        
    @property
    def protected_ranges(self) -> dict[int, tuple[ProtectedRange, ...]]:
        return {k: tuple(self._protected_ranges[k]) for k in self._protected_ranges}
        
    def add_protected_range(self, protected_range: ProtectedRange):
        """ Adds a protected range to the spreadsheet

        Args:
            protected_range (ProtectedRange): the protected range to add

        Raises:
            ValueError: if the id is already taken or there is no sheet for this protected range
        """
        existing_ids = {pr.id for prs in self._protected_ranges.values() for pr in prs if pr.id is not None}
        if protected_range.id in existing_ids:
            raise ValueError("id already taken")
        if protected_range.range.sheet_id not in self._protected_ranges:
            raise ValueError("sheet of this protected_range is not part of this spreadsheet")
        self._protected_ranges[protected_range.range.sheet_id].add(protected_range)
        
    def remove_protected_range(self, protected_range_id: int):
        """ Removes a protected range from the spreadsheet.

        Args:
            protected_range_id (int): id of the protected range

        Raises:
            IndexError: if there is no protected range with the given id
        """
        for sheet_id, protected_ranges in self._protected_ranges.items():
            for protected_range in protected_ranges:
                if protected_range_id == protected_range.id:
                    self._removing_protected_range_ids[sheet_id].add(protected_range_id)
                    protected_ranges.remove(protected_range)
                    return
        raise IndexError(f"id {protected_range_id} not found")
    
    # =========================================================================================================
    
    def sheet_developer_metadata(self) -> dict[int, SheetDeveloperMetadata]:
        return dict(self._developerMetadata)
    
    # =========================================================================================================
    
    def __getitem__(self, key: int | str) -> Sheet:
        if isinstance(key, int):
            return self.get_sheet_by_id(key)
        elif isinstance(key, str):
            return self.get_sheet_by_title(key)
        raise TypeError("key must be of type int or str")
            
    def __setitem__(self, key: int | str, value: Sheet) -> None:
        raise NotImplementedError("use duplicate_sheet")
        
    def __delitem__(self, key: int | str):
        if isinstance(key, int):
            sheet_id = key
        elif isinstance(key, str):
            sheet_id = self.get_sheet_by_title(key).id
        else:
            raise TypeError("key must be of type int or str")
        return self.remove_sheet(sheet_id)

    def __contains__(self, key: object) -> bool:
        try:
            if isinstance(key, int):
                return self.get_sheet_by_id(key) != None
            elif isinstance(key, str):
                return self.get_sheet_by_title(key) != None
        except IndexError:
            return False
        raise TypeError("key must be of type int or str")

    def __iter__(self) -> Iterator:
        return iter(self.sheets)

    def __len__(self) -> int:
        return len(self._sheets)
    
    # =========================================================================================================
    
    @classmethod
    def from_cloud(cls, gc_credentials: BaseCredentials, spreadsheet_id: str) -> Spreadsheet:
        """ Loads a spreadsheet from Google Cloud

        Args:
            gc_credentials (BaseCredentials): authenticated Google Cloud credentials
            spreadsheet_id (str): id of the spreadsheet

        Returns:
            Spreadsheet: the loaded spreadsheet
        """
        gspreadsheet_client = build("sheets", "v4", credentials=gc_credentials).spreadsheets()
        return cls(gspreadsheet_client, spreadsheet_id)
        
    def fetch(self):
        """ Fetches the spreadsheet again from Cloud. Does override all local changes and rebuilds any subclasses """
        self.__init__(self._gspreadsheets_client, self.id)
    
    def push(self):
        """ Pushes all local changes to the Cloud """
        # update values for smaller sheets bc we need to clear those ranges first
        already_existing_downscaled_sheets = {sheet for sheet in self._sheets if (
            sheet.id in set(self._original_sheet_order) and
            sheet.row_count < sheet.initial_row_count or sheet.column_count < sheet.initial_column_count
        )}
        batch_values_update: dict[Sheet, list[ValueRange]] = {}
        for sheet in already_existing_downscaled_sheets:
            if sheet.is_value_dirty:
                assert sheet.fetched_values is not None
                batch_values_update[sheet] = sheet.fetched_values.dirty_cells
        self._batch_values_update(list(chain.from_iterable(batch_values_update.values())))
        for sheet in batch_values_update:
            sheet.mark_value_clean()
        
        # update sheet properties of already existing sheets
        batch_updates: dict[Sheet, list[dict[str, Any]]] = {}
        for sheet in self._sheets:
            if not sheet.is_dirty:
                continue
            if sheet.id not in set(self._original_sheet_order):
                # not yet existing
                continue
            batch_updates.setdefault(sheet, []).append({
                "updateSheetProperties": {
                    "properties": sheet.properties_json,
                    "fields": sheet.dirty_field_mask
                }
            })
        self._batch_update(list(chain.from_iterable(batch_updates.values())))
        for sheet in batch_updates:
            sheet.mark_clean()
        
        # value updates of already existing sheets
        batch_values_update.clear()
        for sheet in self._sheets:
             if not sheet.is_value_dirty:
                 continue
             if sheet.id not in self._original_sheet_order:
                 continue
             assert sheet.fetched_values is not None
             batch_values_update[sheet] = sheet.fetched_values.dirty_cells
        self._batch_values_update(list(chain.from_iterable(batch_values_update.values())))
        for sheet in batch_values_update:
            sheet.mark_value_clean()
        
        # sheet duplication requests
        batch_updates.clear()
        source_sheets: list[Sheet] = []
        for duplicate_sheet_request in self._duplicate_sheet_requests:
            source_sheets.append(self.get_sheet_by_id(duplicate_sheet_request['sourceSheetId']))
            new_sheet = self.get_sheet_by_id(duplicate_sheet_request['newSheetId'])
            batch_updates.setdefault(new_sheet, []).append({
                "duplicateSheet": duplicate_sheet_request
            })
            
            # check for properties updates after duplication
            if new_sheet.is_dirty:
                batch_updates[new_sheet].append({
                "updateSheetProperties": {
                    "properties": new_sheet.properties_json,
                    "fields": new_sheet.dirty_field_mask
                }
            })
        self._batch_update(list(chain.from_iterable(batch_updates.values())))
        for sheet in source_sheets:
            sheet.stale = False
        for sheet in batch_updates:
            sheet.mark_clean()
            
        # value update for duplicated sheet
        batch_values_update.clear()
        for sheet in batch_updates:
            if not sheet.is_value_dirty:
                continue
            assert sheet.fetched_values is not None
            batch_values_update[sheet] = sheet.fetched_values.dirty_cells
        self._batch_values_update(list(chain.from_iterable(batch_values_update.values())))
        for sheet in batch_values_update:
            sheet.mark_value_clean()
        
        batch_meta_updates: dict[int, list[dict[str, Any]]] = {}
        # apply data validation
        for sheet_id, validations in self._data_validations.items():
            for validation in validations:
                batch_meta_updates.setdefault(sheet_id, []).append({
                    "setDataValidation": validation.to_json()
                })
            
        # update protected ranges
        for sheet_id, protected_ranges in self._protected_ranges.items():
            for pr in protected_ranges:
                if not pr.is_dirty:
                    continue
                if pr.id is None:
                    # new protected range
                    batch_meta_updates.setdefault(sheet_id, []).append({
                        "addProtectedRange": {
                            "protectedRange": pr.request_json
                        }
                    })
                else:
                    batch_meta_updates.setdefault(sheet_id, []).append({
                        "updateProtectedRange": {
                            "protectedRange": pr.request_json,
                            "fields": pr.dirty_field_mask
                        }
                    })

        # update developerMetadata            
        for sheet_id, developerMetadata in self._developerMetadata.items():
            if not developerMetadata.is_dirty:
                continue
            if developerMetadata.id is not None:
                data_filter = {
                    "developerMetadataLookup": {
                        "metadataId": developerMetadata.id
                    }
                }
                
                if not developerMetadata:
                    # metadata empty, remove
                    batch_meta_updates.setdefault(sheet_id, []).append({
                        "deleteDeveloperMetadata": {
                            "dataFilter": data_filter
                        }
                    })
                else:
                    # metadata has values, update
                    batch_meta_updates.setdefault(sheet_id, []).append({
                        "updateDeveloperMetadata": {
                            "dataFilters": [data_filter],
                            "developerMetadata": developerMetadata.request_json,
                            "fields": developerMetadata.dirty_field_mask
                        }
                    })
                continue
            # developerMetadata does not exist, create if not empty
            if developerMetadata:
                batch_meta_updates.setdefault(sheet_id, []).append({
                    "createDeveloperMetadata": {
                        "developerMetadata": developerMetadata.request_json
                    }
                })
        batch_requests = list(chain.from_iterable(batch_meta_updates.values()))
        remove_requests = [{"sheetId": sheet_id} for sheet_id in self._removing_sheet_ids]
        batch_requests.extend(remove_requests)
        # check if we need to resort sheets
        current_order = [s.id for s in self._sheets]
        if current_order != self._original_sheet_order:
            for index, sheet in enumerate(self._sheets):
                batch_requests.append({"updateSheetProperties": {"properties": {"sheetId": sheet.id, "index": index}, "fields": "index"}})
        self._batch_update(batch_requests)
        {pr.mark_clean() for pr_list in self._protected_ranges.values() for pr in pr_list if pr.is_dirty}
        {dm.mark_clean() for dm in self._developerMetadata.values() if dm.is_dirty}
        self._duplicate_sheet_requests.clear()
        self._removing_sheet_ids.clear()
        self._original_sheet_order = current_order
    
    def _batch_update(self, requests: list[dict[str, Any]]) -> list[dict[str, Any]]:
        if not requests:
            return []
        response = self._gspreadsheets_client.batchUpdate(
            spreadsheetId=self.id,
            body={
                "requests": requests
            }
        ).execute()
        return response
        
    def _batch_values_update(self, value_ranges: list[ValueRange], value_input_option: str = "USER_ENTERED"):
        if not value_ranges:
            return []
        body = {
            "valueInputOption": value_input_option,
            "data": [vr.to_json() for vr in value_ranges],
            "includeValuesInResponse": False
        }
        response = self._gspreadsheets_client.values().batchUpdate(
            spreadsheetId=self.id,
            body=body
        ).execute()
        return response
    
    def _copy_paste(self, source: GridRange, destinations: list[GridRange], paste_type: str = "PASTE_NORMAL"):
        requests = [{
                "copyPaste": {
                    "source": source.to_json(),
                    "destination": dst.to_json(),
                    "pasteType": paste_type,
                    "pasteOrientation": "NORMAL",
                }
            }
            for dst in destinations
        ]
        return self._batch_update(requests)
    