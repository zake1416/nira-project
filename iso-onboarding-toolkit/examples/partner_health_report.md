# ISO Load Integrity & Trust Report: CAISO

## Executive Summary

- Generated (UTC): 2026-01-16T17:45:00Z
- Trust Score: 86.5 (DEGRADED)
- Issues: HIGH=2, MEDIUM=2, LOW=0

## Coverage & Completeness

- Completeness: 98.72% (3558/3604 rows)
- Missing hours across zones: 18
- Missing zones per hour: 6

## DST Handling Summary

- Total days analyzed: 31
- NORMAL: 29, SPRING_FORWARD: 1, FALL_BACK: 1

## Internal Consistency

- Consistency violations: 4
- Max abs error: 612.45 MW
- Max pct error: 1.74%

## Top Issues

### 1. [HIGH] completeness
- Missing 18 hourly observations across zones. Completeness 98.72%.
- Hint: Identify gaps by date/zone and reconcile missing hours with ISO source files.

### 2. [HIGH] consistency
- 4 hours exceed consistency thresholds (abs>250.0 MW or pct>1.00%).
- Hint: Investigate mismatched totals or zonal corrections in the source feed.

### 3. [MEDIUM] sanity
- 9 hourly changes exceed spike thresholds.
- Hint: Review sudden load jumps and verify they align with ISO event notes.

### 4. [MEDIUM] sanity
- 3 flatline windows detected across zones.
- Hint: Investigate upstream data freezes or missing updates.

## Recommended Actions

- Identify gaps by date/zone and reconcile missing hours with ISO source files.
- Investigate mismatched totals or zonal corrections in the source feed.
- Review sudden load jumps and verify they align with ISO event notes.
