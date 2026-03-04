from __future__ import annotations

import base64
import random
from datetime import date, timedelta

from .models import ExtractRequest


PLANTS = ["1000", "1100", "1200", "1300"]
STORAGE_LOCATIONS = ["0001", "0002", "0003", "0004", "RM01", "FG01"]
MOVEMENT_TYPES = ["201", "261", "311", "551"]
UNITS = ["EA", "KG", "L"]


def _left_pad(value: str, size: int) -> str:
    return value.zfill(size)


def _char(value: str, size: int) -> str:
    return value.ljust(size)


def _format_qty(value: float) -> str:
    return f"{value:0.3f}"


def encode_page_token(offset: int) -> str:
    return base64.urlsafe_b64encode(str(offset).encode("utf-8")).decode("utf-8")


def decode_page_token(token: str | None) -> int:
    if not token:
        return 0
    try:
        return int(base64.urlsafe_b64decode(token.encode("utf-8")).decode("utf-8"))
    except Exception:
        return 0


def build_dataset(days_back: int, records_per_day: int, seed: int) -> list[dict[str, str]]:
    rng = random.Random(seed)
    today = date.today()
    records: list[dict[str, str]] = []

    reservation_base = 500_000_000

    for day_offset in range(days_back + 1):
        business_date = today - timedelta(days=day_offset)
        date_text = business_date.strftime("%Y%m%d")

        for item_idx in range(records_per_day):
            reservation_number = reservation_base + day_offset * records_per_day + item_idx
            reservation_item = (item_idx % 30 + 1) * 10
            material_number = _left_pad(str(1_000_000 + rng.randint(0, 899_999)), 18)
            plant = rng.choice(PLANTS)
            storage_location = rng.choice(STORAGE_LOCATIONS)
            movement_type = rng.choice(MOVEMENT_TYPES)
            batch = f"B{rng.randint(100000, 999999)}"
            requirement_qty = round(rng.uniform(1, 300), 3)
            withdrawn_qty = round(requirement_qty * rng.uniform(0, 1), 3)
            deleted_flag = "X" if rng.random() < 0.03 else ""
            final_issue_flag = "X" if rng.random() < 0.08 else ""
            uom = rng.choice(UNITS)

            record = {
                "MANDT": "100",
                "RSNUM": _left_pad(str(reservation_number), 10),
                "RSPOS": _left_pad(str(reservation_item), 4),
                "MATNR": _char(material_number, 18),
                "WERKS": _char(plant, 4),
                "LGORT": _char(storage_location, 4),
                "CHARG": _char(batch, 10),
                "BWART": _char(movement_type, 3),
                "BDTER": date_text,
                "BDMNG": _format_qty(requirement_qty),
                "ENMNG": _format_qty(withdrawn_qty),
                "MEINS": _char(uom, 3),
                "XLOEK": _char(deleted_flag, 1),
                "KZEAR": _char(final_issue_flag, 1),
            }
            records.append(record)

    records.sort(key=lambda row: (row["BDTER"], row["RSNUM"], row["RSPOS"]))
    return records


def apply_filters(records: list[dict[str, str]], req: ExtractRequest) -> list[dict[str, str]]:
    filtered = [
        row
        for row in records
        if req.from_date <= row["BDTER"] <= req.to_date
    ]

    if req.werks:
        wanted = {item.strip().upper() for item in req.werks}
        filtered = [row for row in filtered if row["WERKS"].strip().upper() in wanted]

    if req.lgort:
        wanted = {item.strip().upper() for item in req.lgort}
        filtered = [row for row in filtered if row["LGORT"].strip().upper() in wanted]

    if req.matnr:
        wanted = {item.strip().lstrip("0") for item in req.matnr}
        filtered = [row for row in filtered if row["MATNR"].strip().lstrip("0") in wanted]

    if req.bwart:
        wanted = {item.strip().upper() for item in req.bwart}
        filtered = [row for row in filtered if row["BWART"].strip().upper() in wanted]

    return filtered
