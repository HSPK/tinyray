"""Every change is checked, but ordinary CI must never build a release."""

import re

from tests.support.registry import ROOT

WORKFLOW = ROOT / ".github/workflows/release.yml"


def test_code_quality_runs_for_pull_requests_and_main():
    text = WORKFLOW.read_text()
    triggers = text.split("\npermissions:", 1)[0]
    assert re.search(r"^  pull_request:\s*$", triggers, re.MULTILINE)
    assert re.search(r"^    branches:\n      - main$", triggers, re.MULTILINE)
    check = re.search(r"^  check:\n(.*?)(?=^  wheels:)", text, re.MULTILINE | re.DOTALL)[1]
    assert not re.search(r"^    if:", check, re.MULTILINE)
    for command in ("cargo test --workspace", "pytest tests/ -q", "ruff check", "mypy"):
        assert command in check


def test_release_jobs_are_not_enabled_by_pull_requests_or_main_pushes():
    text = WORKFLOW.read_text()
    for name in ("wheels", "sdist", "publish"):
        body = re.search(rf"^  {name}:\n(.*?)(?=^  \w|\Z)", text, re.MULTILINE | re.DOTALL)[1]
        guard = re.search(r"^    if: (.+)$", body, re.MULTILINE)[1]
        assert guard in (
            "github.event_name == 'workflow_dispatch' || startsWith(github.ref, 'refs/tags/v')",
            "startsWith(github.ref, 'refs/tags/v') || github.event_name == 'workflow_dispatch'",
        )
