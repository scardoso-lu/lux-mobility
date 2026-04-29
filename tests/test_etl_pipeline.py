"""
Tests for ETL pure functions in etl/pipeline.py.
No DuckDB database, filesystem downloads, or PostgreSQL required.
"""
import textwrap
import tempfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pipeline import (
    detect_latlon_cols,
    detect_address_cols,
    parse_inspire_gml,
    detect_train_crs,
)


# ── detect_latlon_cols ────────────────────────────────────────────────────────

def _mock_con(columns: list[tuple[str, str]]):
    """Return a fake DuckDB connection whose DESCRIBE returns *columns*."""
    con = MagicMock()
    con.execute.return_value.fetchall.return_value = columns
    return con


def test_detect_latlon_standard_wgs84():
    con = _mock_con([("lat_wgs84", "DOUBLE"), ("lon_wgs84", "DOUBLE"), ("addr", "VARCHAR")])
    lat, lon = detect_latlon_cols(con, "t")
    assert lat == "lat_wgs84"
    assert lon == "lon_wgs84"


def test_detect_latlon_generic_names():
    con = _mock_con([("latitude", "DOUBLE"), ("longitude", "DOUBLE")])
    lat, lon = detect_latlon_cols(con, "t")
    assert lat == "latitude"
    assert lon == "longitude"


def test_detect_latlon_short_names():
    con = _mock_con([("lat", "DOUBLE"), ("lon", "DOUBLE"), ("name", "VARCHAR")])
    lat, lon = detect_latlon_cols(con, "t")
    assert lat == "lat"
    assert lon == "lon"


def test_detect_latlon_xy_fallback():
    con = _mock_con([("x", "DOUBLE"), ("y", "DOUBLE")])
    lat, lon = detect_latlon_cols(con, "t")
    assert lat == "y"
    assert lon == "x"


def test_detect_latlon_missing_raises():
    con = _mock_con([("name", "VARCHAR"), ("address", "VARCHAR")])
    with pytest.raises(RuntimeError, match="Cannot detect lat/lon"):
        detect_latlon_cols(con, "t")


def test_detect_latlon_priority_order():
    # lat_wgs84 should win over latitude when both present
    con = _mock_con([("latitude", "DOUBLE"), ("lat_wgs84", "DOUBLE"), ("lon_wgs84", "DOUBLE")])
    lat, _ = detect_latlon_cols(con, "t")
    assert lat == "lat_wgs84"


# ── detect_address_cols ───────────────────────────────────────────────────────

def test_detect_address_luxembourg_dbf():
    con = _mock_con([
        ("numero", "VARCHAR"),
        ("rue", "VARCHAR"),
        ("localite", "VARCHAR"),
        ("code_postal", "VARCHAR"),
    ])
    expr = detect_address_cols(con, "t")
    assert "numero" in expr
    assert "rue" in expr
    assert "localite" in expr
    assert "code_postal" in expr
    assert "COALESCE" in expr
    assert "|| ', ' ||" in expr


def test_detect_address_generic_fields():
    con = _mock_con([
        ("house_number", "VARCHAR"),
        ("street", "VARCHAR"),
        ("city", "VARCHAR"),
        ("postcode", "VARCHAR"),
    ])
    expr = detect_address_cols(con, "t")
    assert "house_number" in expr
    assert "street" in expr
    assert "city" in expr
    assert "postcode" in expr


def test_detect_address_partial_fields():
    # Only street + city available
    con = _mock_con([("rue", "VARCHAR"), ("commune", "VARCHAR")])
    expr = detect_address_cols(con, "t")
    assert "rue" in expr
    assert "commune" in expr


def test_detect_address_fallback_to_first_text_col():
    con = _mock_con([("id", "INTEGER"), ("full_address", "VARCHAR")])
    expr = detect_address_cols(con, "t")
    assert "full_address" in expr


def test_detect_address_no_columns_raises():
    con = _mock_con([("id", "INTEGER"), ("count", "BIGINT")])
    with pytest.raises(RuntimeError, match="Cannot find address columns"):
        detect_address_cols(con, "t")


# ── parse_inspire_gml ─────────────────────────────────────────────────────────

_GML_TEMPLATE = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <root xmlns:gml="http://www.opengis.net/gml/3.2">
      <gml:LineString>
        <gml:posList srsDimension="2">{pos}</gml:posList>
      </gml:LineString>
    </root>
""")


def _write_gml(tmp_path: Path, pos: str) -> Path:
    p = tmp_path / "roads.gml"
    p.write_text(_GML_TEMPLATE.format(pos=pos))
    return p


def test_parse_inspire_gml_single_linestring(tmp_path):
    # Two points in EPSG:3035 Northing-first order: N=3050000, E=750000
    gml = _write_gml(tmp_path, "3050000 750000 3051000 751000")
    result = parse_inspire_gml(gml)
    assert len(result) == 1
    wkt = result[0]
    assert wkt.startswith("LINESTRING")
    # After axis swap: X=750000 Y=3050000
    assert "750000.0 3050000.0" in wkt
    assert "751000.0 3051000.0" in wkt


def test_parse_inspire_gml_multiple_linestrings(tmp_path):
    content = textwrap.dedent("""\
        <?xml version="1.0" encoding="UTF-8"?>
        <root xmlns:gml="http://www.opengis.net/gml/3.2">
          <gml:LineString>
            <gml:posList srsDimension="2">3050000 750000 3051000 751000</gml:posList>
          </gml:LineString>
          <gml:LineString>
            <gml:posList srsDimension="2">3052000 752000 3053000 753000</gml:posList>
          </gml:LineString>
        </root>
    """)
    p = tmp_path / "roads.gml"
    p.write_text(content)
    result = parse_inspire_gml(p)
    assert len(result) == 2


def test_parse_inspire_gml_skips_single_point(tmp_path):
    # Only one point — not a valid linestring; should be skipped
    gml = _write_gml(tmp_path, "3050000 750000")
    result = parse_inspire_gml(gml)
    assert result == []


def test_parse_inspire_gml_empty_file(tmp_path):
    p = tmp_path / "empty.gml"
    p.write_text('<?xml version="1.0"?><root xmlns:gml="http://www.opengis.net/gml/3.2"></root>')
    result = parse_inspire_gml(p)
    assert result == []


def test_parse_inspire_gml_3d_coordinates(tmp_path):
    content = textwrap.dedent("""\
        <?xml version="1.0" encoding="UTF-8"?>
        <root xmlns:gml="http://www.opengis.net/gml/3.2">
          <gml:LineString srsDimension="3">
            <gml:posList srsDimension="3">3050000 750000 300 3051000 751000 310</gml:posList>
          </gml:LineString>
        </root>
    """)
    p = tmp_path / "roads3d.gml"
    p.write_text(content)
    result = parse_inspire_gml(p)
    assert len(result) == 1
    assert "750000.0 3050000.0" in result[0]


# ── detect_train_crs ──────────────────────────────────────────────────────────

def test_detect_train_crs_wgs84(tmp_path):
    (tmp_path / "stops.prj").write_text('GEOGCS["WGS 84",DATUM["WGS_1984"]]')
    assert detect_train_crs(tmp_path) == "EPSG:4326"


def test_detect_train_crs_wgs84_short(tmp_path):
    (tmp_path / "stops.prj").write_text('GEOGCS["GCS_WGS84"]')
    assert detect_train_crs(tmp_path) == "EPSG:4326"


def test_detect_train_crs_web_mercator(tmp_path):
    (tmp_path / "stops.prj").write_text('PROJCS["WGS_1984_Web_Mercator_Auxiliary_Sphere",PROJECTION["Mercator"]]')
    assert detect_train_crs(tmp_path) == "EPSG:3857"


def test_detect_train_crs_no_prj_defaults_to_lu(tmp_path):
    # tmp_path has no .prj file — should fall back to Luxembourg national CRS
    assert detect_train_crs(tmp_path) == "EPSG:2169"


def test_detect_train_crs_unknown_prj_defaults_to_lu(tmp_path):
    (tmp_path / "stops.prj").write_text("PROJCS[\"Some Unknown CRS\"]")
    assert detect_train_crs(tmp_path) == "EPSG:2169"


def test_detect_train_crs_3857_numeric(tmp_path):
    (tmp_path / "stops.prj").write_text('AUTHORITY["EPSG","3857"]')
    assert detect_train_crs(tmp_path) == "EPSG:3857"
