#!/usr/bin/env python3
"""
Preflight verifier for Company Research Agent.

Checks before launching Docker:
- Python syntax errors across the repo
- Import-time ModuleNotFoundError by importing key modules (incl. application)
- Optional Node/Airtable extension dependency resolution (npm dry-run)

Exit codes:
- 0: All checks passed
- 1: Python syntax or import errors
- 2: Node (npm) dependency issues
"""
import os
import sys
import subprocess
import importlib
from pathlib import Path
import py_compile

REPO_ROOT = Path(__file__).parent.resolve()

PYTHON_EXCLUDE_DIRS = {
    '.venv', 'venv', 'env', '__pycache__', '.pytest_cache', '.tox',
    'node_modules', '.tmp', '.git'
}

PY_MODULES_TO_IMPORT = [
    'application',
    'backend.graph',
    'backend.services.email_generator',
    'backend.services.pdf_service',
    'backend.services.websocket_manager',
    'backend.utils.research_parser',
    'backend.airtable_uploader',
]

MISSING_PIP_HINTS = {
    'jwt': 'PyJWT',
    'bs4': 'beautifulsoup4',
    'dotenv': 'python-dotenv',
    'prometheus_client': 'prometheus-client',
    'PyPDF2': 'PyPDF2',
    'markdown': 'markdown',
    'slowapi': 'slowapi',
    'email_validator': 'email-validator',
}

def check_python_syntax() -> list[str]:
    errors: list[str] = []
    for root, dirs, files in os.walk(REPO_ROOT):
        # prune excluded dirs
        dirs[:] = [d for d in dirs if d not in PYTHON_EXCLUDE_DIRS]
        for f in files:
            if f.endswith('.py') and f != 'preflight_check.py':
                path = Path(root) / f
                try:
                    py_compile.compile(str(path), doraise=True)
                except Exception as e:
                    rel = path.relative_to(REPO_ROOT)
                    errors.append(f"[SYNTAX] {rel}: {e}")
    return errors


def check_python_imports() -> list[str]:
    errors: list[str] = []
    sys.path.insert(0, str(REPO_ROOT))
    for mod in PY_MODULES_TO_IMPORT:
        try:
            importlib.import_module(mod)
        except ModuleNotFoundError as e:
            missing = e.name or 'unknown'
            hint = MISSING_PIP_HINTS.get(missing)
            suffix = f" -> try: pip install {hint}" if hint else ""
            errors.append(f"[IMPORT] {mod}: missing module '{missing}'{suffix}")
        except Exception as e:
            errors.append(f"[IMPORT] {mod}: {type(e).__name__}: {e}")
    return errors


def check_node_npm() -> tuple[int, str]:
    """Return (exit_code, output). Uses npm dry-run to validate dependency tree."""
    scripting_dir = REPO_ROOT / 'scripting'
    if not scripting_dir.exists():
        return 0, 'No scripting directory; skipping npm checks.'
    # Prefer a dry-run install with legacy-peer-deps to catch resolution issues without writing
    cmd = ['npm', 'install', '--legacy-peer-deps', '--dry-run']
    try:
        proc = subprocess.run(cmd, cwd=str(scripting_dir), stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=False)
        return proc.returncode, proc.stdout
    except FileNotFoundError:
        return 2, 'npm not found; install Node.js to validate the Airtable extension.'


def main() -> int:
    print('🔎 Preflight: Python syntax check...')
    py_syntax_errors = check_python_syntax()
    if py_syntax_errors:
        for err in py_syntax_errors:
            print(err)
        print('\n❌ Python syntax errors found. Fix these first.')
        return 1
    print('✅ No Python syntax errors.')

    print('\n🔎 Preflight: Python import check...')
    py_import_errors = check_python_imports()
    if py_import_errors:
        for err in py_import_errors:
            print(err)
        print('\n❌ Python import errors detected. Ensure requirements.txt includes the missing packages, then reinstall.')
        return 1
    print('✅ Python imports OK.')

    print('\n🔎 Preflight: Node/Airtable dependency resolution (dry-run)...')
    npm_code, npm_out = check_node_npm()
    if npm_code != 0:
        print(npm_out)
        print('\n❌ Node dependency check failed (see output above).')
        return 2
    print('✅ Node dependency resolution OK (dry-run).')

    print('\n🎉 Preflight passed. You are safe to launch Docker.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
