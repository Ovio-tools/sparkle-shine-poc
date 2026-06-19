# Marks scripts/archive as an importable package so retained regression tests
# (e.g. tests/test_audit_orphan_invoices.py) can import the archived one-off
# scripts they cover. The scripts themselves are retired and not run in prod.
