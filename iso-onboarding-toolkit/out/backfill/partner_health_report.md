# ISO Load Integrity & Trust Report: CAISO

## Executive Summary

- Generated (UTC): 2026-01-17T01:44:16.991142Z
- Trust Score: 0.0 (DO_NOT_USE)
- Issues: HIGH=2, MEDIUM=2, LOW=0

## Coverage & Completeness

- Completeness: 99.71% (87465/87720 rows)
- Missing hours across zones: 265
- Missing zones per hour: 0

## DST Handling Summary

- Total days analyzed: 731
- NORMAL: 727, SPRING_FORWARD: 2, FALL_BACK: 2

## Internal Consistency

- Consistency violations: 41
- Max abs error: 21766.07 MW
- Max pct error: 100.00%

## Top Issues

### 1. [HIGH] normalize
- 17520 rows have invalid hours in 'HE'.
- Hint: Ensure hour column is numeric and within 1-24.

### 2. [HIGH] consistency
- 41 hours exceed consistency thresholds (abs>250.0 MW or pct>1.00%).
- Hint: Investigate mismatched totals or zonal corrections in the source feed.

### 3. [MEDIUM] completeness
- Missing 265 hourly observations across zones. Completeness 99.71%.
- Hint: Identify gaps by date/zone and reconcile missing hours with ISO source files.

### 4. [MEDIUM] sanity
- 10292 hourly changes exceed spike thresholds.
- Hint: Review sudden load jumps and verify they align with ISO event notes.

## Recommended Actions

- Ensure hour column is numeric and within 1-24.
- Investigate mismatched totals or zonal corrections in the source feed.