from __future__ import annotations

from datetime import date, datetime
from typing import Any

from pydantic import BaseModel, Field, field_validator


DATE_FMT = "%Y%m%d"


class ExtractRequest(BaseModel):
    from_date: str = Field(..., description="Start date in YYYYMMDD")
    to_date: str = Field(..., description="End date in YYYYMMDD")
    werks: list[str] | None = Field(default=None, description="Plant filter")
    lgort: list[str] | None = Field(default=None, description="Storage location filter")
    matnr: list[str] | None = Field(default=None, description="Material filter")
    bwart: list[str] | None = Field(default=None, description="Movement type filter")
    page_size: int = Field(default=200, ge=1, le=1000)
    page_token: str | None = Field(default=None, description="Pagination token")

    @field_validator("from_date", "to_date")
    @classmethod
    def validate_dates(cls, value: str) -> str:
        datetime.strptime(value, DATE_FMT)
        return value

    @property
    def from_date_obj(self) -> date:
        return datetime.strptime(self.from_date, DATE_FMT).date()

    @property
    def to_date_obj(self) -> date:
        return datetime.strptime(self.to_date, DATE_FMT).date()


class ExtractResponse(BaseModel):
    request_id: str
    source_ts: str
    total_estimated: int
    next_page_token: str | None
    records: list[dict[str, Any]]


SAP_MB25_FIELDS = [
    "MANDT",
    "RSNUM",
    "RSPOS",
    "MATNR",
    "WERKS",
    "LGORT",
    "CHARG",
    "BWART",
    "BDTER",
    "BDMNG",
    "ENMNG",
    "MEINS",
    "XLOEK",
    "KZEAR",
]
