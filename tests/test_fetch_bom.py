"""Tests for BOM forecast fetching and parsing."""

import csv
import tempfile
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from fetch_bom import (
    LocationForecast,
    parse_bom_xml,
    parse_forecast_period,
    write_csv,
)
import xml.etree.ElementTree as ET


# Minimal XML for testing
MINIMAL_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<product version="1.7">
    <forecast>
        <area aac="NSW_PT131" description="Sydney" type="location">
            <forecast-period index="0" start-time-local="2026-01-08T10:00:00+11:00">
                <element type="forecast_icon_code">1</element>
                <element type="air_temperature_maximum" units="Celsius">33</element>
            </forecast-period>
            <forecast-period index="1" start-time-local="2026-01-09T00:00:00+11:00">
                <element type="air_temperature_minimum" units="Celsius">21</element>
                <element type="air_temperature_maximum" units="Celsius">35</element>
            </forecast-period>
        </area>
    </forecast>
</product>
"""

# XML with multiple locations
MULTI_LOCATION_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<product version="1.7">
    <forecast>
        <area aac="NSW_FA001" description="New South Wales" type="region"/>
        <area aac="NSW_PT131" description="Sydney" type="location">
            <forecast-period index="0" start-time-local="2026-01-08T10:00:00+11:00">
                <element type="air_temperature_maximum" units="Celsius">33</element>
            </forecast-period>
            <forecast-period index="1" start-time-local="2026-01-09T00:00:00+11:00">
                <element type="air_temperature_minimum" units="Celsius">21</element>
                <element type="air_temperature_maximum" units="Celsius">35</element>
            </forecast-period>
        </area>
        <area aac="NSW_PT082" description="Liverpool" type="location">
            <forecast-period index="0" start-time-local="2026-01-08T10:00:00+11:00">
                <element type="air_temperature_maximum" units="Celsius">39</element>
            </forecast-period>
            <forecast-period index="1" start-time-local="2026-01-09T00:00:00+11:00">
                <element type="air_temperature_minimum" units="Celsius">19</element>
                <element type="air_temperature_maximum" units="Celsius">40</element>
            </forecast-period>
        </area>
    </forecast>
</product>
"""

# XML with missing temperature data
MISSING_DATA_XML = b"""<?xml version="1.0" encoding="UTF-8"?>
<product version="1.7">
    <forecast>
        <area aac="NSW_PT999" description="Mystery Location" type="location">
            <forecast-period index="0" start-time-local="2026-01-08T10:00:00+11:00">
                <element type="forecast_icon_code">1</element>
            </forecast-period>
        </area>
    </forecast>
</product>
"""


class TestParseForecastPeriod:
    """Tests for parse_forecast_period function."""

    def test_parses_min_and_max(self):
        xml = """
        <forecast-period index="1" start-time-local="2026-01-09T00:00:00+11:00">
            <element type="air_temperature_minimum" units="Celsius">21</element>
            <element type="air_temperature_maximum" units="Celsius">35</element>
        </forecast-period>
        """
        elem = ET.fromstring(xml)
        date, min_temp, max_temp = parse_forecast_period(elem)

        assert date == "2026-01-09"
        assert min_temp == 21
        assert max_temp == 35

    def test_parses_max_only(self):
        xml = """
        <forecast-period index="0" start-time-local="2026-01-08T10:00:00+11:00">
            <element type="air_temperature_maximum" units="Celsius">33</element>
        </forecast-period>
        """
        elem = ET.fromstring(xml)
        date, min_temp, max_temp = parse_forecast_period(elem)

        assert date == "2026-01-08"
        assert min_temp is None
        assert max_temp == 33

    def test_handles_no_temps(self):
        xml = """
        <forecast-period index="0" start-time-local="2026-01-08T10:00:00+11:00">
            <element type="forecast_icon_code">1</element>
        </forecast-period>
        """
        elem = ET.fromstring(xml)
        date, min_temp, max_temp = parse_forecast_period(elem)

        assert date == "2026-01-08"
        assert min_temp is None
        assert max_temp is None

    def test_handles_invalid_temp_value(self):
        xml = """
        <forecast-period index="0" start-time-local="2026-01-08T10:00:00+11:00">
            <element type="air_temperature_maximum" units="Celsius">invalid</element>
        </forecast-period>
        """
        elem = ET.fromstring(xml)
        date, min_temp, max_temp = parse_forecast_period(elem)

        assert max_temp is None


class TestParseBomXml:
    """Tests for parse_bom_xml function."""

    def test_parses_single_location(self):
        forecasts = list(parse_bom_xml(MINIMAL_XML))

        assert len(forecasts) == 1
        assert forecasts[0].aac == "NSW_PT131"
        assert forecasts[0].description == "Sydney"
        assert forecasts[0].max_temp == 33
        assert forecasts[0].max_temp_date == "2026-01-08"
        assert forecasts[0].min_temp == 21
        assert forecasts[0].min_temp_date == "2026-01-09"

    def test_parses_multiple_locations(self):
        forecasts = list(parse_bom_xml(MULTI_LOCATION_XML))

        assert len(forecasts) == 2

        sydney = forecasts[0]
        assert sydney.description == "Sydney"
        assert sydney.max_temp == 33
        assert sydney.min_temp == 21

        liverpool = forecasts[1]
        assert liverpool.description == "Liverpool"
        assert liverpool.max_temp == 39
        assert liverpool.min_temp == 19

    def test_ignores_non_location_areas(self):
        # The MULTI_LOCATION_XML has a "region" type area that should be ignored
        forecasts = list(parse_bom_xml(MULTI_LOCATION_XML))
        descriptions = [f.description for f in forecasts]

        assert "New South Wales" not in descriptions

    def test_handles_missing_temperatures(self):
        forecasts = list(parse_bom_xml(MISSING_DATA_XML))

        assert len(forecasts) == 1
        assert forecasts[0].description == "Mystery Location"
        assert forecasts[0].min_temp is None
        assert forecasts[0].max_temp is None

    def test_takes_nearest_temps(self):
        # Sydney has max in index 0 (33) and min in index 1 (21)
        # Should take nearest (first available) for each
        forecasts = list(parse_bom_xml(MINIMAL_XML))

        sydney = forecasts[0]
        # Max from index 0
        assert sydney.max_temp == 33
        assert sydney.max_temp_date == "2026-01-08"
        # Min from index 1 (not available in index 0)
        assert sydney.min_temp == 21
        assert sydney.min_temp_date == "2026-01-09"


class TestWriteCsv:
    """Tests for write_csv function."""

    def test_writes_csv_with_header(self):
        forecasts = [
            LocationForecast(
                aac="TEST001",
                description="Test Location",
                min_temp=15,
                min_temp_date="2026-01-09",
                max_temp=25,
                max_temp_date="2026-01-08",
            )
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            output_path = f.name

        try:
            count = write_csv(iter(forecasts), output_path)

            assert count == 1

            with open(output_path) as f:
                reader = csv.reader(f)
                rows = list(reader)

            assert len(rows) == 2  # header + 1 data row
            assert rows[0] == [
                "aac",
                "location",
                "min_temp_celsius",
                "min_temp_date",
                "max_temp_celsius",
                "max_temp_date",
            ]
            assert rows[1] == [
                "TEST001",
                "Test Location",
                "15",
                "2026-01-09",
                "25",
                "2026-01-08",
            ]
        finally:
            Path(output_path).unlink()

    def test_handles_missing_values(self):
        forecasts = [
            LocationForecast(
                aac="TEST001",
                description="Test Location",
                min_temp=None,
                min_temp_date=None,
                max_temp=25,
                max_temp_date="2026-01-08",
            )
        ]

        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            output_path = f.name

        try:
            write_csv(iter(forecasts), output_path)

            with open(output_path) as f:
                reader = csv.reader(f)
                rows = list(reader)

            # Missing values should be empty strings
            assert rows[1][2] == ""  # min_temp_celsius
            assert rows[1][3] == ""  # min_temp_date
        finally:
            Path(output_path).unlink()


class TestIntegration:
    """Integration tests using the real sample data."""

    def test_parses_real_sample_data(self):
        sample_path = Path(__file__).parent / "sample.xml"
        if not sample_path.exists():
            return  # Skip if sample not available

        with open(sample_path, "rb") as f:
            xml_data = f.read()

        forecasts = list(parse_bom_xml(xml_data))

        # Should have many locations
        assert len(forecasts) > 100

        # All should have descriptions
        assert all(f.description for f in forecasts)

        # Most should have temperature data
        with_temps = [f for f in forecasts if f.max_temp is not None]
        assert len(with_temps) > 100
