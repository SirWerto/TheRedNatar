#!/usr/bin/python
import argparse
import csv
from dataclasses import dataclass, asdict
from typing import Optional
from datetime import datetime
import pyarrow as pa
import pyarrow.parquet as pq

import subprocess

schema = pa.schema(
    [
        ("subdomain", pa.string()),
        ("ingestion_datetime", pa.timestamp("us")),
        ("grid_position", pa.int32()),
        ("x_position", pa.int32()),
        ("y_position", pa.int32()),
        ("tribe", pa.int32()),
        ("village_id", pa.int32()),
        ("village_name", pa.string()),
        ("player_id", pa.int32()),
        ("player_name", pa.string()),
        ("alliance_id", pa.int32()),
        ("alliance_name", pa.string()),
        ("population", pa.int32()),
        ("region", pa.string()),  # Optional[str]
        ("is_capital", pa.bool_()),  # Optional[bool]
        ("is_city", pa.bool_()),  # Optional[bool]
        ("has_harbor", pa.bool_()),  # Optional[bool]
        ("victory_points", pa.int32()),  # Optional[int]
    ]
)


@dataclass(frozen=True)
class MapSQLRow:
    subdomain: str
    ingestion_datetime: datetime
    grid_position: int
    x_position: int
    y_position: int
    tribe: int
    village_id: int
    village_name: str
    player_id: int
    player_name: str
    alliance_id: int
    alliance_name: str
    population: int
    region: Optional[str]
    is_capital: Optional[bool]
    is_city: Optional[bool]
    has_harbor: Optional[bool]
    victory_points: Optional[int]

    @staticmethod
    def from_row(
        subdomain: str, ingestion_datetime: datetime, attr: list[str]
    ) -> "MapSQLRow":
        assert len(attr) == 16, f"Malformed row --> {attr}"

        def _parse_bool(str_bool: str) -> bool:
            return True if str_bool.upper() == "TRUE" else False

        return MapSQLRow(
            subdomain=subdomain,
            ingestion_datetime=ingestion_datetime,
            grid_position=int(attr[0]),
            x_position=int(attr[1]),
            y_position=int(attr[2]),
            tribe=int(attr[3]),
            village_id=int(attr[4]),
            village_name=attr[5],
            player_id=int(attr[6]),
            player_name=attr[7],
            alliance_id=int(attr[8]),
            alliance_name=attr[9],
            population=int(attr[10]),
            region=None if "NULL" == attr[11].upper() else attr[11],
            is_capital=None if "NULL" == attr[12].upper() else _parse_bool(attr[12]),
            is_city=None if "NULL" == attr[13].upper() else _parse_bool(attr[13]),
            has_harbor=None if "NULL" == attr[14].upper() else _parse_bool(attr[14]),
            victory_points=None if "NULL" == attr[15].upper() else int(attr[15]),
        )


@dataclass(frozen=True)
class MapSQL:
    subdomain: str

    @staticmethod
    def fetch(subdomain: str) -> list[MapSQLRow]:
        ingestion_datetime = datetime.now()
        result = subprocess.run(
            ["curl", f"https://{subdomain}/map.sql"], capture_output=True, text=True
        )
        if MapSQL.is_valid_map_sql(result.stdout) is False:
            return []

        raw_rows = list(
            csv.reader(
                [row[30:][:-2] for row in result.stdout.splitlines()], quotechar="'"
            )
        )

        rows = [
            MapSQLRow.from_row(subdomain, ingestion_datetime, row) for row in raw_rows
        ]

        return rows

    @staticmethod
    def is_valid_map_sql(output: str) -> bool:
        return True


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Clean, add server column and reorder the collected map.sql from Travian servers"
    )

    parser.add_argument("travian_subdomains_csv_filepath")
    parser.add_argument("destination_folder")

    args = parser.parse_args()

    with open(args.travian_subdomains_csv_filepath, newline="") as csvfile:
        spamreader: list[list[str]] = list(
            csv.reader(csvfile, delimiter=" ", quotechar="|")
        )

        rows = []
        for row in spamreader[1:]:
            print(row[0])
            rows = rows + MapSQL.fetch(row[0])

    table = pa.table(
        list(zip(*[tuple(asdict(r).values()) for r in rows])), schema=schema
    )
    pq.write_table(
        table, args.destination_folder, compression="BROTLI", compression_level=11
    )
