"""
Human-readable audit report generator for the cross-tool data integrity audit.
"""

from __future__ import annotations

import os
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from demo.audit.cross_tool_audit import AuditReport

_DEFAULT_REPORT_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "audit",
    "audit_report.txt",
)


def generate_report(audit_result: "AuditReport") -> str:
    """Generate a human-readable audit report.

    Format:
    ============================================
    CROSS-TOOL DATA INTEGRITY AUDIT
    Run at: {timestamp}
    ============================================

    SUMMARY
    Total checks:    {total}
    Matches:         {matches} ({pass_rate:.1%})
    Mismatches:      {mismatches}
    Missing:         {missing}
    Orphans:         {orphans}

    PASS / WARNING / FAIL verdict + tool breakdown table + critical issues + all findings.
    """
    lines: list[str] = []
    s = audit_result.summary

    # ---- Header ----
    lines.append("=" * 60)
    lines.append("CROSS-TOOL DATA INTEGRITY AUDIT")
    lines.append(f"Run at: {audit_result.timestamp}")
    lines.append("=" * 60)
    lines.append("")

    # ---- Summary ----
    lines.append("SUMMARY")
    lines.append(f"  Total checks:    {s.total_checks}")
    lines.append(f"  Matches:         {s.matches} ({s.pass_rate:.1%})")
    lines.append(f"  Mismatches:      {s.mismatches}")
    lines.append(f"  Missing:         {s.missing}")
    lines.append(f"  Orphans:         {s.orphans}")
    lines.append("")

    if s.pass_rate >= 0.95:
        lines.append("  PASS -- Data integrity is solid.")
    elif s.pass_rate >= 0.85:
        lines.append("  WARNING -- Some issues need attention before the demo.")
    else:
        lines.append("  FAIL -- Significant data inconsistencies found. Fix before demo.")
    lines.append("")

    # ---- Tool breakdown table ----
    lines.append("TOOL BREAKDOWN")
    lines.append(_table_header())
    lines.append(_table_divider())
    for tool_name, result in audit_result.tool_results.items():
        counts = {sev: 0 for sev in ("match", "mismatch", "missing", "orphan")}
        for f in result.findings:
            counts[f.severity] += 1
        total_tool = len(result.findings)
        lines.append(
            f"  {tool_name:<14} {result.records_checked:>8} {counts['match']:>9} "
            f"{counts['mismatch']:>10} {counts['missing']:>9} "
            f"({result.duration_seconds:.1f}s)"
        )
    lines.append(_table_footer())
    lines.append("")

    # ---- Critical issues ----
    if s.critical_issues:
        lines.append("CRITICAL ISSUES  (visible during a live demo)")
        lines.append("-" * 60)
        for i, msg in enumerate(s.critical_issues, start=1):
            lines.append(f"  {i}. {msg}")
        lines.append("")
    else:
        lines.append("CRITICAL ISSUES")
        lines.append("  None -- no demo-blocking issues found.")
        lines.append("")

    # ---- All findings, grouped by tool ----
    lines.append("ALL FINDINGS")
    lines.append("-" * 60)
    for tool_name, result in audit_result.tool_results.items():
        if not result.findings:
            continue
        lines.append(f"\n  [{tool_name.upper()}]  ({len(result.findings)} checks)")
        for f in result.findings:
            sev_label = f.severity.upper().ljust(9)
            entity = f"{f.entity_type}/{f.canonical_id}"
            if f.tool_id:
                entity += f" -> {f.tool_id}"
            if f.field:
                entity += f"  [{f.field}]"
            lines.append(f"    {sev_label}  {entity}")
            if f.message:
                lines.append(f"               {f.message}")

    lines.append("")
    lines.append("=" * 60)
    lines.append("END OF REPORT")
    lines.append("=" * 60)

    return "\n".join(lines)


def save_report(report: str, path: str = _DEFAULT_REPORT_PATH) -> None:
    """Save the report to disk for review."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        fh.write(report)
    print(f"[audit] Report saved to {path}")


# ---------------------------------------------------------------------------
# Table formatting helpers
# ---------------------------------------------------------------------------

def _table_header() -> str:
    return (
        "  " + "-" * 56 + "\n"
        "  | Tool           | Checked  | Match    | Mismatch | Missing  |\n"
        "  " + "-" * 56
    )


def _table_divider() -> str:
    return "  " + "-" * 56


def _table_footer() -> str:
    return "  " + "-" * 56
