import os
import tempfile
import uuid

import pandas as pd

from models import BenchmarkPoint, Team, db
from services.benchmark_parser import parse_benchmarks


def _create_temp_excel(data):
    df = pd.DataFrame(data)
    fd, path = tempfile.mkstemp(suffix=".xlsx")
    os.close(fd)
    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="Benchmarks")
    return path


def _create_team(name_prefix="Benchmark Team"):
    suffix = uuid.uuid4().hex[:8]
    team = Team(name=f"{name_prefix} {suffix}", slug=f"{name_prefix.lower().replace(' ', '-')}-{suffix}")
    db.session.add(team)
    db.session.commit()
    return team


def test_parse_benchmarks_valid(app_context):
    team = _create_team()
    data = {
        "Asset Class": ["Buyout", "Buyout", "Buyout", "Buyout"],
        "Vintage Year": [2019, 2019, 2019, 2019],
        "Metric": ["Net IRR", "Net IRR", "Net IRR", "Net IRR"],
        "Quartile": ["Lower Quartile", "Median", "Upper Quartile", "Top 5%"],
        "Value": [0.12, 0.16, 0.21, 0.29],
    }
    file_path = _create_temp_excel(data)
    try:
        result = parse_benchmarks(file_path, team_id=team.id, replace_mode="replace_all")
        assert result["success"] is True
        assert result["rows_loaded"] == 4
        assert result["asset_classes"] == ["Buyout"]
        assert result["vintage_min"] == 2019
        assert result["vintage_max"] == 2019
        assert BenchmarkPoint.query.filter_by(team_id=team.id).count() == 4
    finally:
        os.remove(file_path)


def test_parse_benchmarks_metric_alias_net_tvpi_maps_to_net_moic(app_context):
    team = _create_team()
    data = {
        "Asset Class": ["Buyout"],
        "Vintage Year": [2020],
        "Metric": ["Net TVPI"],
        "Quartile": ["Median"],
        "Value": [1.9],
    }
    file_path = _create_temp_excel(data)
    try:
        result = parse_benchmarks(file_path, team_id=team.id, replace_mode="replace_all")
        assert result["success"] is True
        point = BenchmarkPoint.query.filter_by(team_id=team.id).first()
        assert point is not None
        assert point.metric == "net_moic"
    finally:
        os.remove(file_path)


def test_parse_benchmarks_quartile_normalization(app_context):
    team = _create_team()
    data = {
        "Asset Class": ["Growth", "Growth", "Growth", "Growth"],
        "Vintage Year": [2018, 2018, 2018, 2018],
        "Metric": ["Net DPI", "Net DPI", "Net DPI", "Net DPI"],
        "Quartile": ["Lower Quartile", "Median", "Upper Quartile", "Top 5%"],
        "Value": [0.5, 0.8, 1.1, 1.6],
    }
    file_path = _create_temp_excel(data)
    try:
        result = parse_benchmarks(file_path, team_id=team.id, replace_mode="replace_all")
        assert result["success"] is True
        quartiles = {
            row.quartile
            for row in BenchmarkPoint.query.filter_by(team_id=team.id).all()
        }
        assert quartiles == {"lower_quartile", "median", "upper_quartile", "top_5"}
    finally:
        os.remove(file_path)


def test_parse_benchmarks_duplicate_key_rows_fail(app_context):
    team = _create_team()
    data = {
        "Asset Class": ["Buyout", "Buyout"],
        "Vintage Year": [2019, 2019],
        "Metric": ["Net IRR", "Net IRR"],
        "Quartile": ["Median", "Median"],
        "Value": [0.16, 0.17],
    }
    file_path = _create_temp_excel(data)
    try:
        result = parse_benchmarks(file_path, team_id=team.id, replace_mode="replace_all")
        assert result["success"] is False
        assert result["rows_loaded"] == 0
        assert any("Duplicate benchmark key" in err for err in result["errors"])
        assert BenchmarkPoint.query.filter_by(team_id=team.id).count() == 0
    finally:
        os.remove(file_path)


def test_parse_benchmarks_full_replace_behavior(app_context):
    team = _create_team()
    file_path_a = _create_temp_excel(
        {
            "Asset Class": ["Buyout", "Buyout"],
            "Vintage Year": [2018, 2018],
            "Metric": ["Net IRR", "Net IRR"],
            "Quartile": ["Median", "Upper Quartile"],
            "Value": [0.14, 0.2],
        }
    )
    file_path_b = _create_temp_excel(
        {
            "Asset Class": ["Growth"],
            "Vintage Year": [2021],
            "Metric": ["Net DPI"],
            "Quartile": ["Median"],
            "Value": [0.9],
        }
    )
    try:
        result_a = parse_benchmarks(file_path_a, team_id=team.id, replace_mode="replace_all")
        assert result_a["success"] is True
        assert BenchmarkPoint.query.filter_by(team_id=team.id).count() == 2

        result_b = parse_benchmarks(file_path_b, team_id=team.id, replace_mode="replace_all")
        assert result_b["success"] is True
        rows = BenchmarkPoint.query.filter_by(team_id=team.id).all()
        assert len(rows) == 1
        assert rows[0].asset_class == "Growth"
        assert rows[0].metric == "net_dpi"
    finally:
        os.remove(file_path_a)
        os.remove(file_path_b)
