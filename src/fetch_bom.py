#!/usr/bin/env python3
"""
Fetch BOM weather data and convert to CSV.

Streams XML from BOM FTP server and extracts the nearest min/max temperatures
for each location.
"""

import csv
import subprocess
import sys
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from datetime import datetime
from io import BytesIO
from typing import Iterator


BOM_URL = "ftp://ftp.bom.gov.au/anon/gen/fwo/IDN11060.xml"


@dataclass
class LocationForecast:
    """Forecast data for a single location."""
    aac: str
    description: str
    min_temp: int | None
    min_temp_date: str | None
    max_temp: int | None
    max_temp_date: str | None


def fetch_xml_data(url: str = BOM_URL) -> bytes:
    """Fetch XML data from BOM FTP server using curl."""
    result = subprocess.run(
        ["curl", "-s", url],
        capture_output=True,
        check=True,
    )
    return result.stdout


def parse_forecast_period(period_elem: ET.Element) -> tuple[str | None, int | None, int | None]:
    """
    Parse a forecast period element.

    Returns (start_date, min_temp, max_temp).
    """
    start_date = period_elem.get("start-time-local")
    if start_date:
        # Extract just the date part (YYYY-MM-DD)
        start_date = start_date[:10]

    min_temp = None
    max_temp = None

    for element in period_elem.findall("element"):
        elem_type = element.get("type")
        if elem_type == "air_temperature_minimum":
            try:
                min_temp = int(element.text)
            except (ValueError, TypeError):
                pass
        elif elem_type == "air_temperature_maximum":
            try:
                max_temp = int(element.text)
            except (ValueError, TypeError):
                pass

    return start_date, min_temp, max_temp


def parse_bom_xml(xml_data: bytes) -> Iterator[LocationForecast]:
    """
    Stream parse BOM XML and yield location forecasts.

    For each location, finds the nearest (lowest index) forecast period
    containing min and max temperatures.
    """
    # Use iterparse for streaming - we process area elements one at a time
    context = ET.iterparse(BytesIO(xml_data), events=("end",))

    for event, elem in context:
        if elem.tag == "area" and elem.get("type") == "location":
            aac = elem.get("aac", "")
            description = elem.get("description", "")

            # Find nearest min and max temps
            nearest_min: int | None = None
            nearest_min_date: str | None = None
            nearest_max: int | None = None
            nearest_max_date: str | None = None

            # Forecast periods are already in index order in the XML
            for period in elem.findall("forecast-period"):
                start_date, min_temp, max_temp = parse_forecast_period(period)

                # Take the first (nearest) min temp we find
                if nearest_min is None and min_temp is not None:
                    nearest_min = min_temp
                    nearest_min_date = start_date

                # Take the first (nearest) max temp we find
                if nearest_max is None and max_temp is not None:
                    nearest_max = max_temp
                    nearest_max_date = start_date

                # Stop once we have both
                if nearest_min is not None and nearest_max is not None:
                    break

            yield LocationForecast(
                aac=aac,
                description=description,
                min_temp=nearest_min,
                min_temp_date=nearest_min_date,
                max_temp=nearest_max,
                max_temp_date=nearest_max_date,
            )

            # Clear the element to free memory
            elem.clear()


def write_csv(forecasts: Iterator[LocationForecast], output_path: str) -> int:
    """Write forecasts to CSV file. Returns number of rows written."""
    count = 0
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "aac",
            "location",
            "min_temp_celsius",
            "min_temp_date",
            "max_temp_celsius",
            "max_temp_date",
        ])

        for forecast in forecasts:
            writer.writerow([
                forecast.aac,
                forecast.description,
                forecast.min_temp if forecast.min_temp is not None else "",
                forecast.min_temp_date or "",
                forecast.max_temp if forecast.max_temp is not None else "",
                forecast.max_temp_date or "",
            ])
            count += 1

    return count


def main() -> int:
    """Main entry point."""
    output_path = "site/forecast.csv"

    print(f"Fetching data from {BOM_URL}...")
    xml_data = fetch_xml_data()
    print(f"Downloaded {len(xml_data):,} bytes")

    print(f"Parsing XML and writing to {output_path}...")
    forecasts = parse_bom_xml(xml_data)
    count = write_csv(forecasts, output_path)

    print(f"Done! Wrote {count} locations to {output_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
