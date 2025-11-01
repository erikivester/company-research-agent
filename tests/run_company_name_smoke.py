import sys
def company_name(state: dict) -> str:
    """Local copy of helper to validate behavior without importing package deps."""
    try:
        name = state.get('company') if isinstance(state, dict) else None
    except Exception:
        name = None
    if not name or not isinstance(name, str) or not name.strip():
        # Try inferred_company next
        try:
            name = state.get('inferred_company') if isinstance(state, dict) else None
        except Exception:
            name = None
    if not name or not isinstance(name, str) or not name.strip():
        return 'Unknown Company'
    return name.strip()

cases = [
    ({'company':'Acme Corp'}, 'Acme Corp'),
    ({'company':'', 'inferred_company':'Inferred Co'}, 'Inferred Co'),
    ({'company':None, 'inferred_company':None}, 'Unknown Company'),
    (None, 'Unknown Company'),
    ({'company':'   ', 'inferred_company':'  '}, 'Unknown Company'),
]

for state, expected in cases:
    res = company_name(state)
    if res != expected:
        print(f"FAIL: state={state!r} expected={expected!r} got={res!r}")
        sys.exit(2)
print("All checks passed")
sys.exit(0)
