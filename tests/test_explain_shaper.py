"""Tests for build_explain_graph.

The shaper is pure (no DB access), so we exercise it against captured fixture
rows that mirror what MonitorSQLSteps returns after row coercion.
"""

import json
from pathlib import Path

import pytest

from tdwm_mcp.fnc_tools_visualize import (
    _confidence_label,
    build_explain_graph,
)

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "explain"


def load_fixture(name: str) -> list[dict]:
    return json.loads((FIXTURE_DIR / name).read_text())


# Confidence label coverage ----------------------------------------------------


@pytest.mark.parametrize(
    "raw, expected",
    [
        (0, "LOW"),
        (1, "HIGH"),
        (2, "NO"),
        (3, "JOIN"),
        ("0", "LOW"),
        ("1", "HIGH"),
        ("2", "NO"),
        ("3", "JOIN"),
        ("H", "HIGH"),
        ("L", "LOW"),
        ("N", "NO"),
        ("J", "JOIN"),
        ("low", "LOW"),
        ("HIGH", "HIGH"),
        (None, "UNKNOWN"),
        ("", "UNKNOWN"),
        ("Z", "UNKNOWN"),
        ("9", "UNKNOWN"),
        (99, "UNKNOWN"),
    ],
)
def test_confidence_label(raw, expected):
    assert _confidence_label(raw) == expected


# linear_simple — happy path ---------------------------------------------------


def test_linear_simple_shape():
    rows = load_fixture("linear_simple.json")
    g = build_explain_graph(rows, session_no=12345)
    assert g["title"] == "Visual EXPLAIN — session 12345"
    assert len(g["nodes"]) == 5
    assert len(g["links"]) == 4
    assert g["meta"]["total_steps"] == 5
    assert g["meta"]["session_no"] == 12345
    assert g["meta"]["row_count"] == 5

    # Nodes preserve input order via numeric step_num.
    assert [n["id"] for n in g["nodes"]] == ["1", "2", "3", "4", "5"]
    assert [n["name"] for n in g["nodes"]] == ["Step 1", "Step 2", "Step 3", "Step 4", "Step 5"]

    # Confidence codes 1,1,0,3,2 → HIGH,HIGH,LOW,JOIN,NO
    assert [n["category"] for n in g["nodes"]] == ["HIGH", "HIGH", "LOW", "JOIN", "NO"]

    # Edges are linear and ordered.
    assert g["links"] == [
        {"source": "1", "target": "2"},
        {"source": "2", "target": "3"},
        {"source": "3", "target": "4"},
        {"source": "4", "target": "5"},
    ]

    # Tooltip contains the step text (truncated long ones), confidence label,
    # and formatted numbers.
    t = g["nodes"][2]["tooltip"]
    assert "Step 3" in t
    assert "Confidence: LOW" in t
    assert "Est rows: 9,800" in t
    assert "Act rows: 12,000" in t

    # Aggregated meta sums elapsed times.
    assert g["meta"]["est_elapsed_cs"] == 1 + 4500 + 1200 + 200 + 1
    assert g["meta"]["act_elapsed_cs"] == 1 + 4200 + 1150 + 195 + 1
    assert g["meta"]["max_est_rows"] == 9800


# negative_est_rows — Teradata's "unknown" sentinel ---------------------------


def test_negative_est_rows_does_not_blow_up_symbolsize_scale():
    rows = load_fixture("negative_est_rows.json")
    g = build_explain_graph(rows, session_no=1)
    assert len(g["nodes"]) == 3
    # The negative est_rows is preserved for inspection but `value` (the
    # symbolSize driver) is clamped to 0 so the bundle still renders sanely.
    assert g["nodes"][1]["est_rows"] == -1
    assert g["nodes"][1]["value"] == 0
    assert g["nodes"][2]["est_rows"] == -9999999
    assert g["nodes"][2]["value"] == 0
    # max_est_rows ignores negatives (defaults to 0).
    assert g["meta"]["max_est_rows"] == 0


# string_confidence — letters / unknown codes --------------------------------


def test_string_confidence_letters():
    rows = load_fixture("string_confidence.json")
    g = build_explain_graph(rows, session_no=1)
    assert [n["category"] for n in g["nodes"]] == [
        "HIGH",
        "LOW",
        "JOIN",
        "NO",
        "UNKNOWN",  # "Z" is unrecognized
    ]


# Boundary cases --------------------------------------------------------------


def test_single_step_has_no_edges():
    rows = load_fixture("single_step.json")
    g = build_explain_graph(rows, session_no=1)
    assert len(g["nodes"]) == 1
    assert g["links"] == []


def test_empty_plan():
    rows = load_fixture("empty.json")
    g = build_explain_graph(rows, session_no=42)
    assert g["nodes"] == []
    assert g["links"] == []
    assert g["meta"]["total_steps"] == 0
    assert g["meta"]["est_elapsed_cs"] == 0
    assert g["meta"]["max_est_rows"] == 0
    assert g["meta"]["session_no"] == 42


def test_no_session_no():
    """Shaper still produces a sane title when no sessionNo is supplied."""
    g = build_explain_graph([], session_no=None)
    assert g["title"] == "Visual EXPLAIN"
    assert g["meta"]["session_no"] is None


# Missing / malformed fields -------------------------------------------------


def test_missing_fields_yields_em_dash_tooltips_and_unknown_confidence():
    rows = load_fixture("missing_fields.json")
    g = build_explain_graph(rows, session_no=1)
    assert len(g["nodes"]) == 3
    n0 = g["nodes"][0]
    assert n0["category"] == "UNKNOWN"
    assert n0["est_rows"] is None
    assert n0["act_rows"] is None
    # Tooltip falls back to "—" for nulls.
    assert "Est rows: —" in n0["tooltip"]
    assert "Act rows: —" in n0["tooltip"]
    # Row with null SQLStep still produces a node; step_text becomes "".
    assert g["nodes"][1]["step_text"] == ""
    # Row with no SQLStep key at all also tolerated.
    assert g["nodes"][2]["step_text"] == ""


def test_unordered_rows_are_sorted_by_step_num():
    rows = load_fixture("unordered.json")
    g = build_explain_graph(rows, session_no=1)
    assert [n["id"] for n in g["nodes"]] == ["1", "2", "3", "4", "5"]
    assert g["nodes"][0]["step_text"].startswith("First")
    assert g["nodes"][2]["step_text"].startswith("Third")


def test_format_strings_with_leading_spaces():
    """Teradata FORMAT'9' returns space-padded strings; shaper must coerce."""
    rows = load_fixture("format_strings.json")
    g = build_explain_graph(rows, session_no=1)
    assert len(g["nodes"]) == 3
    assert [n["id"] for n in g["nodes"]] == ["1", "2", "3"]
    assert [n["category"] for n in g["nodes"]] == ["HIGH", "HIGH", "LOW"]
    assert g["nodes"][1]["est_rows"] == 1200
    assert g["nodes"][2]["est_rows"] == 9800


def test_rows_without_step_num_are_dropped():
    rows = load_fixture("dropped_no_step_num.json")
    g = build_explain_graph(rows, session_no=1)
    # 3 input rows, 1 dropped (missing Num).
    assert len(g["nodes"]) == 2
    assert [n["id"] for n in g["nodes"]] == ["1", "2"]


# Tooltip truncation ----------------------------------------------------------


def test_long_step_text_is_truncated_in_tooltip():
    long_text = "very long step text " * 50  # ~1000 chars
    g = build_explain_graph(
        [{"SQLStep": long_text, "Num": 1, "C": 1, "ERC": 100, "ARC": 100, "EET": 10, "AET": 10}]
    )
    tooltip = g["nodes"][0]["tooltip"]
    # Tooltip body excluding header lines stays under the cap.
    assert "…" in tooltip
    # Long text itself doesn't exceed cap (320 chars).
    body_lines = tooltip.split("\n")
    step_text_line = body_lines[2]
    assert len(step_text_line) <= 320
