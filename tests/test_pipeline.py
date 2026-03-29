"""
Tests for dataflow_agents — no LLM or network required.

Covers:
  - Pure functions in runner.py (_abs, _random_script, _extract_error)
  - Filesystem helpers in runner.py (_collect_files, _collect_previews)
  - Result dataclasses (StepResult, ValidationResult)
  - All four agent tools (write_file, read_file, list_files, run_script)
"""
import pytest
from pathlib import Path

from dataflow_agents.runner import (
    StepResult,
    ValidationResult,
    _abs,
    _collect_files,
    _collect_previews,
    _extract_error,
    _random_script,
)


# ── _abs ──────────────────────────────────────────────────────────────────────

class TestAbs:
    def test_absolute_path_returned_unchanged(self, tmp_path):
        assert _abs(str(tmp_path)) == tmp_path

    def test_relative_path_resolved_against_project_root(self):
        result = _abs("data/raw/test")
        assert result.is_absolute()
        assert result.parts[-3:] == ("data", "raw", "test")


# ── _random_script ────────────────────────────────────────────────────────────

class TestRandomScript:
    def test_two_calls_produce_different_paths(self):
        assert _random_script("ds", "scrape") != _random_script("ds", "scrape")

    def test_path_contains_dataset_and_stem(self):
        path = _random_script("my_dataset", "scrape")
        assert "my_dataset" in path
        assert "scrape" in path

    def test_path_ends_with_py(self):
        assert _random_script("ds", "clean").endswith(".py")


# ── _extract_error ────────────────────────────────────────────────────────────

class TestExtractError:
    def test_picks_up_error_keyword(self):
        assert "ERROR" in _extract_error("line 1\nERROR: boom\nline 3")

    def test_picks_up_traceback(self):
        log = "normal\nTraceback (most recent call last):\n  File x.py\nValueError"
        assert "Traceback" in _extract_error(log)

    def test_picks_up_exit_code(self):
        assert "EXIT CODE" in _extract_error("Running\nEXIT CODE: 1 (FAILED)")

    def test_no_errors_returns_tail(self):
        long = "a" * 600
        assert len(_extract_error(long)) <= 500


# ── _collect_files ────────────────────────────────────────────────────────────

class TestCollectFiles:
    def test_missing_dir_returns_empty(self, tmp_path):
        assert _collect_files(str(tmp_path / "nonexistent")) == []

    def test_empty_dir_returns_empty(self, tmp_path):
        assert _collect_files(str(tmp_path)) == []

    def test_finds_files(self, tmp_path):
        (tmp_path / "a.csv").write_text("col\n1")
        (tmp_path / "b.csv").write_text("col\n2")
        files = _collect_files(str(tmp_path))
        assert len(files) == 2
        assert any("a.csv" in f for f in files)

    def test_entry_includes_byte_size(self, tmp_path):
        (tmp_path / "f.csv").write_text("data")
        assert "bytes" in _collect_files(str(tmp_path))[0]


# ── _collect_previews ─────────────────────────────────────────────────────────

class TestCollectPreviews:
    def test_missing_dir_returns_empty(self, tmp_path):
        assert _collect_previews(str(tmp_path / "nope")) == {}

    def test_returns_preview_for_csv(self, tmp_path):
        (tmp_path / "out.csv").write_text("name,score\nAlice,95\n")
        previews = _collect_previews(str(tmp_path))
        assert len(previews) == 1

    def test_max_files_cap(self, tmp_path):
        for i in range(5):
            (tmp_path / f"f{i}.csv").write_text("a\n1\n")
        assert len(_collect_previews(str(tmp_path), max_files=2)) == 2


# ── StepResult / ValidationResult ────────────────────────────────────────────

class TestDataclasses:
    def test_step_result_defaults(self):
        r = StepResult(success=True)
        assert r.files == []
        assert r.previews == {}
        assert r.agent_log == ""
        assert r.error == ""

    def test_step_result_failure(self):
        r = StepResult(success=False, error="boom")
        assert not r.success
        assert r.error == "boom"

    def test_validation_result_all_passed(self):
        checks = [{"name": "row_count", "passed": True, "detail": "10 rows"}]
        r = ValidationResult(success=True, checks=checks, agent_log="ok")
        assert r.success
        assert len(r.checks) == 1
        assert r.error == ""

    def test_validation_result_failure(self):
        r = ValidationResult(success=False, checks=[], agent_log="", error="no report")
        assert not r.success
        assert r.error == "no report"


# ── write_file tool ───────────────────────────────────────────────────────────

class TestWriteFile:
    def test_creates_file(self, tmp_path):
        from dataflow_agents.tools import write_file
        p = str(tmp_path / "out.txt")
        result = write_file.invoke({"path": p, "content": "hello"})
        assert Path(p).exists()
        assert "Written" in result

    def test_creates_nested_dirs(self, tmp_path):
        from dataflow_agents.tools import write_file
        p = str(tmp_path / "a" / "b" / "c.txt")
        write_file.invoke({"path": p, "content": "x"})
        assert Path(p).exists()

    def test_reports_char_count(self, tmp_path):
        from dataflow_agents.tools import write_file
        content = "hello world"
        result = write_file.invoke({"path": str(tmp_path / "f.txt"), "content": content})
        assert str(len(content)) in result


# ── read_file tool ────────────────────────────────────────────────────────────

class TestReadFile:
    def test_reads_text_file(self, tmp_path):
        from dataflow_agents.tools import read_file
        p = tmp_path / "hello.txt"
        p.write_text("line 1\nline 2")
        assert "line 1" in read_file.invoke({"path": str(p)})

    def test_missing_file_returns_error(self, tmp_path):
        from dataflow_agents.tools import read_file
        assert "does not exist" in read_file.invoke({"path": str(tmp_path / "nope.txt")})

    def test_csv_rendered_as_dataframe_preview(self, tmp_path):
        from dataflow_agents.tools import read_file
        p = tmp_path / "data.csv"
        p.write_text("name,score\nAlice,95\nBob,87\n")
        assert "Alice" in read_file.invoke({"path": str(p)})

    def test_long_file_truncated(self, tmp_path):
        from dataflow_agents.tools import read_file
        p = tmp_path / "big.txt"
        p.write_text("\n".join(f"line {i}" for i in range(200)))
        assert "more lines" in read_file.invoke({"path": str(p)})


# ── list_files tool ───────────────────────────────────────────────────────────

class TestListFiles:
    def test_empty_dir(self, tmp_path):
        from dataflow_agents.tools import list_files
        assert "empty" in list_files.invoke({"directory": str(tmp_path)})

    def test_missing_dir(self, tmp_path):
        from dataflow_agents.tools import list_files
        assert "does not exist" in list_files.invoke({"directory": str(tmp_path / "nope")})

    def test_lists_all_files(self, tmp_path):
        from dataflow_agents.tools import list_files
        (tmp_path / "a.csv").write_text("data")
        (tmp_path / "b.pdf").write_bytes(b"pdf")
        result = list_files.invoke({"directory": str(tmp_path)})
        assert "a.csv" in result
        assert "b.pdf" in result

    def test_includes_byte_sizes(self, tmp_path):
        from dataflow_agents.tools import list_files
        (tmp_path / "f.txt").write_text("hello")
        assert "bytes" in list_files.invoke({"directory": str(tmp_path)})


# ── run_script tool ───────────────────────────────────────────────────────────

class TestRunScript:
    def test_successful_script(self, tmp_path):
        from dataflow_agents.tools import run_script
        p = tmp_path / "ok.py"
        p.write_text("print('success')")
        assert "success" in run_script.invoke({"path": str(p)})

    def test_failing_script_reports_error(self, tmp_path):
        from dataflow_agents.tools import run_script
        p = tmp_path / "fail.py"
        p.write_text("raise ValueError('intentional')")
        result = run_script.invoke({"path": str(p)})
        assert "FAILED" in result or "intentional" in result

    def test_missing_script_returns_error(self, tmp_path):
        from dataflow_agents.tools import run_script
        assert "ERROR" in run_script.invoke({"path": str(tmp_path / "nope.py")})

    def test_script_can_write_csv(self, tmp_path):
        from dataflow_agents.tools import run_script
        out = tmp_path / "out.csv"
        script = tmp_path / "write.py"
        script.write_text(
            f"import pandas as pd\n"
            f"pd.DataFrame({{'a':[1,2],'b':[3,4]}}).to_csv(r'{out}', index=False)\n"
        )
        run_script.invoke({"path": str(script)})
        assert out.exists()
