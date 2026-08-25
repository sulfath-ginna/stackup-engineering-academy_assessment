# Data Quality Report — projects

**Generated:** 2026-08-25T11:38:48

## Summary

- Checks run: 9
- Checks passed: 6
- Checks failed: 3

## Results

| Check | Status |
|---|---|
| completeness | FAIL |
| uniqueness | PASS |
| validity_numeric | PASS |
| validity_date | PASS |
| consistency | PASS |
| referential_integrity | PASS |
| distribution | FAIL |
| freshness | PASS |
| outliers | FAIL |

## Details

### completeness

**Status:** FAIL

```text
{'status': 'FAIL', 'threshold': 0.9, 'details': {'project_id': 1.0, 'project_name': 1.0, 'department': 1.0, 'status': 1.0, 'start_date': 0.87, 'end_date': 0.428, 'budget': 0.936, 'actual_cost': 0.88, 'project_manager_id': 1.0, 'priority': 1.0, 'region': 1.0}, 'failed_columns': ['start_date', 'end_date', 'actual_cost']}
```

### uniqueness

**Status:** PASS

```text
{'status': 'PASS', 'details': 'project_id: 500 non-duplicate rows / 500 total; 0 rows involved in duplicates', 'duplicate_rows': 0}
```

### validity_numeric

**Status:** PASS

```text
{'status': 'PASS', 'details': {'budget': {'minimum': 0, 'maximum': 10000000, 'below_minimum': 0, 'above_maximum': 0}, 'actual_cost': {'minimum': 0, 'maximum': 10000000, 'below_minimum': 0, 'above_maximum': 0}}, 'failed_columns': []}
```

### validity_date

**Status:** PASS

```text
{'status': 'PASS', 'details': {'start_date': {'invalid_dates': 0, 'future_dates': 0, 'future_dates_allowed': False}, 'end_date': {'invalid_dates': 0, 'future_dates': 0, 'future_dates_allowed': True}}, 'failed_columns': []}
```

### consistency

**Status:** PASS

```text
{'status': 'PASS', 'details': [{'rule': {'type': 'before', 'columns': ['start_date', 'end_date']}, 'status': 'PASS', 'violations': 0, 'description': 'start_date < end_date: 0 violations'}, {'rule': {'type': 'non_negative', 'column': 'actual_cost'}, 'status': 'PASS', 'violations': 0, 'description': 'actual_cost >= 0: 0 violations'}, {'rule': {'type': 'non_negative', 'column': 'budget'}, 'status': 'PASS', 'violations': 0, 'description': 'budget >= 0: 0 violations'}], 'failed_rules': []}
```

### referential_integrity

**Status:** PASS

```text
{'status': 'PASS', 'details': {'project_manager_id': {'reference': 'employees.employee_id', 'invalid_count': 0, 'invalid_examples': []}}, 'failed_columns': []}
```

### distribution

**Status:** FAIL

```text
{'status': 'FAIL', 'details': {'status': {'top_value': 'Completed', 'top_share': 0.428, 'threshold': 0.3}, 'department': {'top_value': 'HR', 'top_share': 0.098, 'threshold': 0.3}, 'region': {'top_value': 'Abu Dhabi', 'top_share': 0.44, 'threshold': 0.3}}, 'failed_columns': ['status', 'region']}
```

### freshness

**Status:** PASS

```text
{'status': 'PASS', 'details': 'No freshness rule configured.'}
```

### outliers

**Status:** FAIL

```text
{'status': 'FAIL', 'details': {'budget': {'mean': 564636.75, 'std_dev': 605415.32, 'outlier_count': 0}, 'actual_cost': {'mean': 515605.47, 'std_dev': 607555.16, 'outlier_count': 2}}, 'failed_columns': ['actual_cost']}
```
