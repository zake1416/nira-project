# ISO Load Integrity & Trust Report: CAISO

## Executive Summary

- Generated (UTC): 2026-01-17T01:44:15.326234Z
- Trust Score: 0.0 (DO_NOT_USE)
- Issues: HIGH=2, MEDIUM=4, LOW=0

## Coverage & Completeness

- Completeness: 99.81% (69350/69480 rows)
- Missing hours across zones: 130
- Missing zones per hour: 0

## DST Handling Summary

- Total days analyzed: 579
- NORMAL: 577, SPRING_FORWARD: 1, FALL_BACK: 1

## Internal Consistency

- Consistency violations: 0
- Max abs error: 1.64 MW
- Max pct error: 0.01%

## Top Issues

### 1. [HIGH] normalize
- 34 rows have unparseable dates in 'Date'.
- Hint: Verify date format or normalize upstream before ingestion.

### 2. [HIGH] normalize
- 34 rows have invalid hours in 'HR'.
- Hint: Ensure hour column is numeric and within 1-24.

### 3. [MEDIUM] normalize
- Configured hour column missing; auto-detected HR column.
- Hint: Confirm whether CAISO file uses HE or HR semantics.

### 4. [MEDIUM] normalize
- Configured total zone missing; auto-mapped CAISO to CAISO Total.
- Hint: Verify whether total column should be CAISO or CAISO Total.

### 5. [MEDIUM] completeness
- Missing 130 hourly observations across zones. Completeness 99.81%.
- Hint: Identify gaps by date/zone and reconcile missing hours with ISO source files.

### 6. [MEDIUM] sanity
- 13031 hourly changes exceed spike thresholds.
- Hint: Review sudden load jumps and verify they align with ISO event notes.

## Recommended Actions

- Verify date format or normalize upstream before ingestion.
- Ensure hour column is numeric and within 1-24.