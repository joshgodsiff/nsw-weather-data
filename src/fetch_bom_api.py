#!/usr/bin/env python3
"""
Fetch BOM weather data via the public API and convert to CSV.

Uses the BOM API to get:
- Today's min/max temperature
- 7-day forecast min/max temperatures
- Fire danger rating
- Estimated rainfall
"""

import csv
import json
import sys
import urllib.request
import urllib.error
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterator


BOM_API_BASE = "https://api.weather.bom.gov.au/v1"


@dataclass
class DayForecast:
    """Forecast for a single day."""
    date: str
    temp_min: int | None
    temp_max: int | None


@dataclass
class LocationForecast:
    """Full forecast data for a location."""
    name: str
    geohash: str
    state: str
    postcode: str

    # Today's data
    today_min: int | None
    today_max: int | None
    fire_danger: str | None
    rain_min_mm: float | None
    rain_max_mm: float | None
    rain_chance: int | None

    # 7-day forecast
    daily_forecasts: list[DayForecast] = field(default_factory=list)


def fetch_json(url: str) -> dict:
    """Fetch JSON from a URL."""
    request = urllib.request.Request(
        url,
        headers={"User-Agent": "bom-forecast/1.0"}
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def search_location(name: str) -> dict | None:
    """
    Search for a location by name.

    Returns the first matching location or None if not found.
    """
    # Need at least 3 characters for the API
    if len(name) < 3:
        return None

    url = f"{BOM_API_BASE}/locations?search={urllib.request.quote(name)}"

    try:
        data = fetch_json(url)
        locations = data.get("data", [])

        if not locations:
            return None

        # Try to find an exact match first (case-insensitive)
        name_lower = name.lower()
        for loc in locations:
            if loc.get("name", "").lower() == name_lower:
                return loc

        # Fall back to first result
        return locations[0]
    except urllib.error.URLError as e:
        print(f"Error searching for '{name}': {e}", file=sys.stderr)
        return None


def fetch_daily_forecast(geohash: str) -> list[dict]:
    """Fetch the 7-day daily forecast for a location."""
    url = f"{BOM_API_BASE}/locations/{geohash}/forecasts/daily"

    try:
        data = fetch_json(url)
        return data.get("data", [])
    except urllib.error.URLError as e:
        print(f"Error fetching forecast for '{geohash}': {e}", file=sys.stderr)
        return []


def parse_forecast(location: dict, forecast_data: list[dict]) -> LocationForecast:
    """Parse API response into a LocationForecast."""
    # Extract today's data (first item in forecast)
    today = forecast_data[0] if forecast_data else {}

    # Today's temperatures
    # temp_min is often null for today (already passed), use now.temp_later for overnight
    today_max = today.get("temp_max")
    today_min = today.get("temp_min")

    # If we have "now" data, prefer those values
    now_data = today.get("now", {})
    if now_data:
        if now_data.get("now_label") == "Max":
            today_max = now_data.get("temp_now", today_max)
            today_min = now_data.get("temp_later", today_min)  # Overnight min
        else:
            today_min = now_data.get("temp_now", today_min)
            today_max = now_data.get("temp_later", today_max)

    # Fire danger
    fire_danger = today.get("fire_danger")

    # Rainfall
    rain_data = today.get("rain", {})
    rain_amount = rain_data.get("amount", {})
    rain_min = rain_amount.get("min")
    rain_max = rain_amount.get("max")
    rain_chance = rain_data.get("chance")

    # 7-day forecast
    daily_forecasts = []
    for day in forecast_data:
        date = day.get("date", "")[:10]  # Extract YYYY-MM-DD
        daily_forecasts.append(DayForecast(
            date=date,
            temp_min=day.get("temp_min"),
            temp_max=day.get("temp_max"),
        ))

    return LocationForecast(
        name=location.get("name", ""),
        geohash=location.get("geohash", ""),
        state=location.get("state", ""),
        postcode=location.get("postcode", ""),
        today_min=today_min,
        today_max=today_max,
        fire_danger=fire_danger,
        rain_min_mm=rain_min,
        rain_max_mm=rain_max,
        rain_chance=rain_chance,
        daily_forecasts=daily_forecasts,
    )


def read_locations(path: str) -> list[str]:
    """Read location names from a file, one per line."""
    with open(path) as f:
        return [line.strip() for line in f if line.strip()]


def fetch_forecasts(location_names: list[str]) -> Iterator[LocationForecast]:
    """Fetch forecasts for a list of location names."""
    for name in location_names:
        print(f"Fetching {name}...", file=sys.stderr)

        location = search_location(name)
        if not location:
            print(f"  Warning: Location '{name}' not found", file=sys.stderr)
            continue

        geohash = location.get("geohash")
        if not geohash:
            print(f"  Warning: No geohash for '{name}'", file=sys.stderr)
            continue

        forecast_data = fetch_daily_forecast(geohash)
        if not forecast_data:
            print(f"  Warning: No forecast data for '{name}'", file=sys.stderr)
            continue

        yield parse_forecast(location, forecast_data)


def write_csv(forecasts: Iterator[LocationForecast], output_path: str) -> int:
    """Write forecasts to CSV. Returns number of rows written."""
    count = 0

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)

        # Header - today's data plus 7 days of forecasts
        header = [
            "location",
            "state",
            "postcode",
            "geohash",
            "today_min_c",
            "today_max_c",
            "fire_danger",
            "rain_min_mm",
            "rain_max_mm",
            "rain_chance_pct",
        ]
        # Add columns for each forecast day
        for i in range(7):
            header.extend([f"day{i}_date", f"day{i}_min_c", f"day{i}_max_c"])

        writer.writerow(header)

        for forecast in forecasts:
            row = [
                forecast.name,
                forecast.state,
                forecast.postcode,
                forecast.geohash,
                forecast.today_min if forecast.today_min is not None else "",
                forecast.today_max if forecast.today_max is not None else "",
                forecast.fire_danger or "",
                forecast.rain_min_mm if forecast.rain_min_mm is not None else "",
                forecast.rain_max_mm if forecast.rain_max_mm is not None else "",
                forecast.rain_chance if forecast.rain_chance is not None else "",
            ]

            # Add 7 days of forecasts
            for i in range(7):
                if i < len(forecast.daily_forecasts):
                    day = forecast.daily_forecasts[i]
                    row.extend([
                        day.date,
                        day.temp_min if day.temp_min is not None else "",
                        day.temp_max if day.temp_max is not None else "",
                    ])
                else:
                    row.extend(["", "", ""])

            writer.writerow(row)
            count += 1

    return count


def main() -> int:
    """Main entry point."""
    locations_file = "locations.txt"
    output_path = "site/forecast.csv"

    if not Path(locations_file).exists():
        print(f"Error: {locations_file} not found", file=sys.stderr)
        return 1

    print(f"Reading locations from {locations_file}...", file=sys.stderr)
    location_names = read_locations(locations_file)
    print(f"Found {len(location_names)} locations", file=sys.stderr)

    print(f"Fetching forecasts...", file=sys.stderr)
    forecasts = fetch_forecasts(location_names)
    count = write_csv(forecasts, output_path)

    print(f"Done! Wrote {count} locations to {output_path}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
