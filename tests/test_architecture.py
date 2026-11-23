import os
import shutil
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run_lint_imports(cwd: Path, pythonpath: Path) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "PYTHONPATH": str(pythonpath)}
    return subprocess.run(  # noqa: S603
        [shutil.which("lint-imports") or "lint-imports", "--no-cache"],
        capture_output=True,
        text=True,
        cwd=cwd,
        env=env,
        check=False,
    )


def test_import_contracts_hold() -> None:
    result = run_lint_imports(ROOT, ROOT / "src")
    assert result.returncode == 0, result.stdout


def test_forbidden_contract_actually_bites(tmp_path: Path) -> None:
    shutil.copytree(ROOT / "src", tmp_path / "src")
    shutil.copy(ROOT / "pyproject.toml", tmp_path / "pyproject.toml")
    domain_init = tmp_path / "src" / "subscriptions_service" / "domain" / "__init__.py"
    domain_init.write_text(domain_init.read_text() + "\nimport fastapi  # noqa\n")

    result = run_lint_imports(tmp_path, tmp_path / "src")

    assert result.returncode != 0
    assert "Inner layers are framework- and config-free BROKEN" in result.stdout
