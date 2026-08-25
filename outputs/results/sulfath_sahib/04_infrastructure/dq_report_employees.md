# Data Quality Report — employees

**Generated:** 2026-08-25T11:38:48

## Summary

- Checks run: 9
- Checks passed: 4
- Checks failed: 5

## Results

| Check | Status |
|---|---|
| completeness | PASS |
| uniqueness | PASS |
| validity_numeric | FAIL |
| validity_date | FAIL |
| consistency | FAIL |
| referential_integrity | PASS |
| distribution | FAIL |
| freshness | PASS |
| outliers | FAIL |

## Details

### completeness

**Status:** PASS

```text
{'status': 'PASS', 'threshold': 0.85, 'details': {'employee_id': 1.0, 'full_name': 1.0, 'email': 0.99, 'department': 1.0, 'role': 1.0, 'level': 1.0, 'hire_date': 1.0, 'salary': 1.0, 'manager_id': 1.0, 'region': 1.0, 'status': 1.0, 'years_experience': 1.0}, 'failed_columns': []}
```

### uniqueness

**Status:** PASS

```text
{'status': 'PASS', 'details': 'employee_id: 1000 non-duplicate rows / 1000 total; 0 rows involved in duplicates', 'duplicate_rows': 0}
```

### validity_numeric

**Status:** FAIL

```text
{'status': 'FAIL', 'details': {'salary': {'minimum': 10000, 'maximum': 100000, 'below_minimum': 0, 'above_maximum': 0}, 'years_experience': {'minimum': 0, 'maximum': 50, 'below_minimum': 5, 'above_maximum': 0}}, 'failed_columns': ['years_experience']}
```

### validity_date

**Status:** FAIL

```text
{'status': 'FAIL', 'details': {'hire_date': {'invalid_dates': 8, 'future_dates': 0, 'future_dates_allowed': False}}, 'failed_columns': ['hire_date']}
```

### consistency

**Status:** FAIL

```text
{'status': 'FAIL', 'details': [{'rule': {'type': 'non_negative', 'column': 'salary'}, 'status': 'PASS', 'violations': 0, 'description': 'salary >= 0: 0 violations'}, {'rule': {'type': 'non_negative', 'column': 'years_experience'}, 'status': 'FAIL', 'violations': 5, 'description': 'years_experience >= 0: 5 violations'}, {'rule': {'type': 'not_self_reference', 'column': 'manager_id', 'reference_column': 'employee_id'}, 'status': 'FAIL', 'violations': 1, 'description': 'manager_id != employee_id: 1 violations'}], 'failed_rules': ['years_experience >= 0: 5 violations', 'manager_id != employee_id: 1 violations']}
```

### referential_integrity

**Status:** PASS

```text
{'status': 'PASS', 'details': {'manager_id': {'reference': 'employees.employee_id', 'invalid_count': 0, 'invalid_examples': []}}, 'failed_columns': []}
```

### distribution

**Status:** FAIL

```text
{'status': 'FAIL', 'details': {'department': {'top_value': 'Sales', 'top_share': 0.098, 'threshold': 0.3}, 'role': {'top_value': 'Data Analyst', 'top_share': 0.071, 'threshold': 0.3}, 'level': {'top_value': 'Mid', 'top_share': 0.366, 'threshold': 0.3}, 'region': {'top_value': 'Abu Dhabi', 'top_share': 0.434, 'threshold': 0.3}, 'status': {'top_value': 'Active', 'top_share': 0.96, 'threshold': 0.3}}, 'failed_columns': ['level', 'region', 'status']}
```

### freshness

**Status:** PASS

```text
{'status': 'PASS', 'details': 'No freshness rule configured.'}
```

### outliers

**Status:** FAIL

```text
{'status': 'FAIL', 'details': {'salary': {'mean': 26461.53, 'std_dev': 11297.05, 'outlier_count': 17}, 'years_experience': {'mean': 6.26, 'std_dev': 4.67, 'outlier_count': 13}}, 'failed_columns': ['salary', 'years_experience']}
```
