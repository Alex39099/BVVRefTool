from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

from helper.google_api.sheets.grid_range import GridRange
from helper.google_api.sheets.sheet import Sheet


@dataclass(frozen=True, kw_only=True)
class ValidationRule(ABC):
    input_message: str = ""
    strict: bool = True
    show_custom_ui: bool = True
    
    @property
    @abstractmethod
    def condition_json(self) -> dict[str, Any]:
        ...
        
    def to_json(self) -> dict[str, Any]:
        return {
            "condition": self.condition_json,
            "inputMessage": self.input_message,
            "strict": self.strict,
            "showCustomUi": True
        }

@dataclass(frozen=True)
class DropDownListValidationRule(ValidationRule):
    values: tuple[str, ...]
    
    def __post_init__(self):
        for v in self.values:
            if not isinstance(v, str):
                raise TypeError(f"values must be a tuple of strings, got {self.values}")
            if v.startswith("="):
                raise ValueError("formulas are not supported for list validation")
    
    @property
    def condition_json(self) -> dict[str, Any]:
        return {
            "type": "ONE_OF_LIST",
            "values": [{
                "userEnteredValue": v
            } for v in self.values]
        }

@dataclass(frozen=True)
class DropDownRangeValidationRule(ValidationRule):
    sheet: Sheet
    range: GridRange
    
    def __post_init__(self):
        if self.sheet.id != self.range.sheet_id:
            raise ValueError("range does not belong to given sheet")
        
    @property
    def condition_json(self) -> dict[str, Any]:
        bound_range = GridRange(
            sheet_id=self.sheet.id,
            start_row_idx=self.range.start_row_idx or 0,
            end_row_idx=min(self.sheet.row_count - 1, self.range.end_row_idx or self.sheet.row_count - 1),
            start_column_idx=self.range.start_column_idx or 0,
            end_column_idx=min(self.sheet.column_count - 1, self.range.end_column_idx or self.sheet.column_count - 1)
        )
        a1_notation_range = bound_range.to_a1_notation(sheet_title=self.sheet.title,fixed=True)
        return {
            "type": "ONE_OF_RANGE",
            "values": [{
                "userEnteredValue": f"={a1_notation_range}"
            }]
        }

@dataclass(frozen=True)
class DataValidation:
    sheet: Sheet
    range: GridRange
    rule: ValidationRule | None
    
    def __post_init__(self):
        if isinstance(self.rule, DropDownRangeValidationRule):
            if self.rule.range.overlaps(self.range):
                raise ValueError("validation range and rule range must not overlap")
            if self.rule.range.sheet_id not in {sheet_id for sheet_id in self.sheet.spreadsheet.sheets}:
                raise ValueError("validation rule is outside the underlying spreadsheet")
        if self.range.sheet_id != self.sheet.id:
            raise ValueError("range does not belong to given sheet")
    
    def to_json(self):
        bound_range = GridRange(
            sheet_id=self.sheet.id,
            start_row_idx=self.range.start_row_idx or 0,
            end_row_idx=min(self.sheet.row_count - 1, self.range.end_row_idx or self.sheet.row_count - 1),
            start_column_idx=self.range.start_column_idx or 0,
            end_column_idx=min(self.sheet.column_count - 1, self.range.end_column_idx or self.sheet.column_count - 1)
        )
        return {
            "range": bound_range.to_json(),
            "rule": self.rule.to_json() if self.rule is not None else {},
            "filteredRowsIncluded": True
        }
