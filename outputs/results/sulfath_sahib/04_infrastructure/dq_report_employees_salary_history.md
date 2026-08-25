# Data Quality Report — employees_salary_history

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
{'status': 'FAIL', 'threshold': 0.85, 'details': {'employee_id': 1.0, 'previous_salary': 0.6747, 'new_salary': 1.0, 'previous_role': 0.6747, 'new_role': 1.0, 'previous_level': 0.6747, 'new_level': 1.0, 'effective_date': 1.0, 'change_type': 1.0, 'change_reason': 1.0}, 'failed_columns': ['previous_salary', 'previous_role', 'previous_level']}
```

### uniqueness

**Status:** PASS

```text
{'status': 'PASS', 'details': 'No primary key uniqueness rule configured.'}
```

### validity_numeric

**Status:** PASS

```text
{'status': 'PASS', 'details': {'previous_salary': {'minimum': 0, 'maximum': 100000, 'below_minimum': 0, 'above_maximum': 0}, 'new_salary': {'minimum': 0, 'maximum': 100000, 'below_minimum': 0, 'above_maximum': 0}}, 'failed_columns': []}
```

### validity_date

**Status:** PASS

```text
{'status': 'PASS', 'details': {'effective_date': {'invalid_dates': 0, 'future_dates': 0, 'future_dates_allowed': False}}, 'failed_columns': []}
```

### consistency

**Status:** PASS

```text
{'status': 'PASS', 'details': [{'rule': {'type': 'non_negative', 'column': 'previous_salary'}, 'status': 'PASS', 'violations': 0, 'description': 'previous_salary >= 0: 0 violations'}, {'rule': {'type': 'non_negative', 'column': 'new_salary'}, 'status': 'PASS', 'violations': 0, 'description': 'new_salary >= 0: 0 violations'}], 'failed_rules': []}
```

### referential_integrity

**Status:** PASS

```text
{'status': 'PASS', 'details': {'employee_id': {'reference': 'employees.employee_id', 'invalid_count': 0, 'invalid_examples': []}}, 'failed_columns': []}
```

### distribution

**Status:** FAIL

```text
{'status': 'FAIL', 'details': {'change_type': {'top_value': 'Annual Raise', 'top_share': 0.3494, 'threshold': 0.3}, 'previous_level': {'top_value': 'Junior', 'top_share': 0.681, 'threshold': 0.3}, 'new_level': {'top_value': 'Junior', 'top_share': 0.5181, 'threshold': 0.3}}, 'failed_columns': ['change_type', 'previous_level', 'new_level']}
```

### freshness

**Status:** PASS

```text
{'status': 'PASS', 'details': 'No freshness rule configured.'}
```

### outliers

**Status:** FAIL

```text
{'status': 'FAIL', 'details': {'previous_salary': {'mean': 18758.83, 'std_dev': 8296.14, 'outlier_count': 17}, 'new_salary': {'mean': 21263.38, 'std_dev': 9997.59, 'outlier_count': 22}}, 'failed_columns': ['previous_salary', 'new_salary']}
```
