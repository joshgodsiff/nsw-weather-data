"""Tests for BOM API forecast fetching."""

import csv
import tempfile
from pathlib import Path
from unittest.mock import patch

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fetch_bom_api import (
    DayForecast,
    LocationForecast,
    parse_forecast,
    read_locations,
    write_csv,
    search_location,
    fetch_daily_forecast,
)


# Sample API responses
SEARCH_RESPONSE = {
    "metadata": {"response_timestamp": "2026-01-17T00:00:00Z"},
    "data": [
        {
            "geohash": "r64c839",
            "id": "Lithgow-r64c839",
            "name": "Lithgow",
            "postcode": "2790",
            "state": "NSW"
        }
    ]
}

SEARCH_RESPONSE_MULTIPLE = {
    "metadata": {"response_timestamp": "2026-01-17T00:00:00Z"},
    "data": [
        {
            "geohash": "r1qqfxb",
            "id": "Newbridge-r1qqfxb",
            "name": "Newbridge",
            "postcode": "3551",
            "state": "VIC"
        },
        {
            "geohash": "r64xxxx",
            "id": "Newbridge-r64xxxx",
            "name": "Newbridge",
            "postcode": "2795",
            "state": "NSW"
        }
    ]
}

FORECAST_RESPONSE = {
    "data": [
        {
            "date": "2026-01-16T13:00:00Z",
            "temp_max": 18,
            "temp_min": None,
            "fire_danger": "Moderate",
            "rain": {
                "amount": {
                    "min": 10,
                    "max": 30,
                    "lower_range": 1,
                    "upper_range": 30,
                    "units": "mm"
                },
                "chance": 90
            },
            "now": {
                "is_night": False,
                "now_label": "Max",
                "later_label": "Overnight min",
                "temp_now": 18,
                "temp_later": 12
            }
        },
        {
            "date": "2026-01-17T13:00:00Z",
            "temp_max": 16,
            "temp_min": 12,
            "fire_danger": "Moderate",
            "rain": {
                "amount": {"min": 10, "max": 20, "lower_range": 3, "upper_range": 20, "units": "mm"},
                "chance": 80
            }
        },
        {
            "date": "2026-01-18T13:00:00Z",
            "temp_max": 22,
            "temp_min": 14,
            "fire_danger": "High",
            "rain": {
                "amount": {"min": 0, "max": 5, "lower_range": 0, "upper_range": 5, "units": "mm"},
                "chance": 40
            }
        }
    ],
    "metadata": {"response_timestamp": "2026-01-17T00:00:00Z"}
}

LOCATION_DATA = {
    "geohash": "r64c839",
    "id": "Lithgow-r64c839",
    "name": "Lithgow",
    "postcode": "2790",
    "state": "NSW"
}


class TestParseForecast:
    """Tests for parse_forecast function."""

    def test_parses_today_temps_from_now_data(self):
        forecast = parse_forecast(LOCATION_DATA, FORECAST_RESPONSE["data"])

        # Should use now.temp_now for max and now.temp_later for min
        assert forecast.today_max == 18
        assert forecast.today_min == 12

    def test_parses_fire_danger(self):
        forecast = parse_forecast(LOCATION_DATA, FORECAST_RESPONSE["data"])

        assert forecast.fire_danger == "Moderate"

    def test_parses_rainfall_as_range_string(self):
        forecast = parse_forecast(LOCATION_DATA, FORECAST_RESPONSE["data"])

        # Should format as "lower-upper"
        assert forecast.rain_range_mm == "1-30"

    def test_rainfall_zero_when_both_zero(self):
        data = [{
            "date": "2026-01-16T13:00:00Z",
            "temp_max": 25,
            "rain": {"amount": {"lower_range": 0, "upper_range": 0}}
        }]
        forecast = parse_forecast(LOCATION_DATA, data)

        assert forecast.rain_range_mm == "0"

    def test_parses_location_name(self):
        forecast = parse_forecast(LOCATION_DATA, FORECAST_RESPONSE["data"])

        assert forecast.name == "Lithgow"

    def test_parses_following_days_forecasts(self):
        forecast = parse_forecast(LOCATION_DATA, FORECAST_RESPONSE["data"])

        # Should have 2 following days (skipping today)
        assert len(forecast.daily_forecasts) == 2

        day1 = forecast.daily_forecasts[0]
        assert day1.temp_max == 16
        assert day1.temp_min == 12

        day2 = forecast.daily_forecasts[1]
        assert day2.temp_max == 22
        assert day2.temp_min == 14

    def test_handles_empty_forecast(self):
        forecast = parse_forecast(LOCATION_DATA, [])

        assert forecast.today_max is None
        assert forecast.today_min is None
        assert forecast.fire_danger is None
        assert len(forecast.daily_forecasts) == 0

    def test_handles_missing_rain_data(self):
        data = [{"date": "2026-01-16T13:00:00Z", "temp_max": 25}]
        forecast = parse_forecast(LOCATION_DATA, data)

        assert forecast.rain_range_mm == ""

    def test_handles_night_now_data(self):
        """When it's night, now_label is 'Min' and temp order is reversed."""
        night_data = [{
            "date": "2026-01-16T13:00:00Z",
            "temp_max": 18,
            "temp_min": 10,
            "now": {
                "is_night": True,
                "now_label": "Min",
                "later_label": "Max",
                "temp_now": 10,
                "temp_later": 25
            }
        }]
        forecast = parse_forecast(LOCATION_DATA, night_data)

        assert forecast.today_min == 10
        assert forecast.today_max == 25


class TestReadLocations:
    """Tests for read_locations function."""

    def test_reads_locations_from_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("Sydney\nMelbourne\nBrisbane\n")
            f.flush()
            path = f.name

        try:
            locations = read_locations(path)
            assert locations == ["Sydney", "Melbourne", "Brisbane"]
        finally:
            Path(path).unlink()

    def test_skips_blank_lines(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("Sydney\n\nMelbourne\n  \nBrisbane\n")
            f.flush()
            path = f.name

        try:
            locations = read_locations(path)
            assert locations == ["Sydney", "Melbourne", "Brisbane"]
        finally:
            Path(path).unlink()

    def test_strips_whitespace(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
            f.write("  Sydney  \n  Melbourne\n")
            f.flush()
            path = f.name

        try:
            locations = read_locations(path)
            assert locations == ["Sydney", "Melbourne"]
        finally:
            Path(path).unlink()


class TestWriteCsv:
    """Tests for write_csv function."""

    def test_writes_header_and_data(self):
        forecasts = [
            LocationForecast(
                name="Lithgow",
                today_min=12,
                today_max=18,
                daily_forecasts=[
                    DayForecast(temp_min=12, temp_max=16),
                    DayForecast(temp_min=14, temp_max=22),
                ],
                rain_range_mm="1-30",
                fire_danger="Moderate",
            )
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name

        try:
            count = write_csv(iter(forecasts), path)
            assert count == 1

            with open(path) as f:
                reader = csv.reader(f)
                rows = list(reader)

            assert len(rows) == 2  # header + 1 data row

            # Check header structure
            header = rows[0]
            assert header[0] == "location"
            assert header[1] == "today_min"
            assert header[2] == "today_max"
            assert "day1_min" in header
            assert "rain_mm" in header
            assert "fire_danger" in header

            # Check data
            data = rows[1]
            assert data[0] == "Lithgow"
            assert data[1] == "12"  # today_min
            assert data[2] == "18"  # today_max

            # Fire danger should be last
            assert data[-1] == "Moderate"
        finally:
            Path(path).unlink()

    def test_handles_missing_values(self):
        forecasts = [
            LocationForecast(
                name="Test",
                today_min=None,
                today_max=25,
                fire_danger=None,
                rain_range_mm="",
            )
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name

        try:
            write_csv(iter(forecasts), path)

            with open(path) as f:
                reader = csv.reader(f)
                rows = list(reader)

            # Missing values should be empty strings
            data = rows[1]
            assert data[1] == ""  # today_min
            assert data[-1] == ""  # fire_danger
        finally:
            Path(path).unlink()

    def test_pads_missing_forecast_days(self):
        """Should have 6 following days even if fewer are provided."""
        forecasts = [
            LocationForecast(
                name="Test",
                today_min=10,
                today_max=20,
                daily_forecasts=[
                    DayForecast(temp_min=10, temp_max=20),
                ],
                rain_range_mm="0-5",
                fire_danger="Low",
            )
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name

        try:
            write_csv(iter(forecasts), path)

            with open(path) as f:
                reader = csv.reader(f)
                rows = list(reader)

            # Should have header with all 6 following days
            header = rows[0]
            assert "day6_min" in header
            assert "day6_max" in header

            # Data row should be padded
            assert len(rows[1]) == len(header)
        finally:
            Path(path).unlink()

    def test_field_order(self):
        """Fields should be: location, today, days 1-6, rain, fire."""
        forecasts = [
            LocationForecast(
                name="Test",
                today_min=10,
                today_max=20,
                daily_forecasts=[DayForecast(temp_min=11, temp_max=21)],
                rain_range_mm="0-5",
                fire_danger="Moderate",
            )
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            path = f.name

        try:
            write_csv(iter(forecasts), path)

            with open(path) as f:
                reader = csv.reader(f)
                header = next(reader)

            # Check order
            assert header.index("location") == 0
            assert header.index("today_min") == 1
            assert header.index("today_max") == 2
            assert header.index("day1_min") == 3
            assert header.index("rain_mm") < header.index("fire_danger")
            assert header.index("fire_danger") == len(header) - 1
        finally:
            Path(path).unlink()


class TestSearchLocation:
    """Tests for search_location function."""

    def test_returns_none_for_short_name(self):
        result = search_location("AB")
        assert result is None

    @patch("fetch_bom_api.fetch_json")
    def test_returns_exact_match(self, mock_fetch):
        mock_fetch.return_value = SEARCH_RESPONSE_MULTIPLE
        result = search_location("Newbridge")

        assert result is not None
        # Should match first one since both have same name
        assert result["name"] == "Newbridge"

    @patch("fetch_bom_api.fetch_json")
    def test_returns_first_result_if_no_exact_match(self, mock_fetch):
        mock_fetch.return_value = {
            "data": [
                {"name": "Sydney CBD", "geohash": "abc"},
                {"name": "Sydney Airport", "geohash": "def"},
            ]
        }
        result = search_location("Sydney")

        assert result["name"] == "Sydney CBD"

    @patch("fetch_bom_api.fetch_json")
    def test_returns_none_if_no_results(self, mock_fetch):
        mock_fetch.return_value = {"data": []}
        result = search_location("NonexistentPlace")

        assert result is None


class TestFetchDailyForecast:
    """Tests for fetch_daily_forecast function."""

    @patch("fetch_bom_api.fetch_json")
    def test_returns_forecast_data(self, mock_fetch):
        mock_fetch.return_value = FORECAST_RESPONSE
        result = fetch_daily_forecast("r64c839")

        assert len(result) == 3
        assert result[0]["temp_max"] == 18

    @patch("fetch_bom_api.fetch_json")
    def test_returns_empty_list_on_error(self, mock_fetch):
        import urllib.error
        mock_fetch.side_effect = urllib.error.URLError("Network error")
        result = fetch_daily_forecast("r64c839")

        assert result == []
