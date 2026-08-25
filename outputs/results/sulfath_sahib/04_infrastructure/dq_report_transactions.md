# Data Quality Report — transactions

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
{'status': 'FAIL', 'threshold': 0.8, 'details': {'transaction_id': 1.0, 'project_id': 1.0, 'vendor_id': 1.0, 'vendor_name': 1.0, 'category': 1.0, 'amount': 0.9852, 'currency': 1.0, 'transaction_date': 1.0, 'approved_by': 0.9511, 'payment_status': 1.0, 'invoice_ref': 1.0, 'notes': 0.785}, 'failed_columns': ['notes']}
```

### uniqueness

**Status:** PASS

```text
{'status': 'PASS', 'details': 'transaction_id: 50000 non-duplicate rows / 50000 total; 0 rows involved in duplicates', 'duplicate_rows': 0}
```

### validity_numeric

**Status:** PASS

```text
{'status': 'PASS', 'details': {'amount': {'minimum': 0, 'maximum': 10000000, 'below_minimum': 0, 'above_maximum': 0}}, 'failed_columns': []}
```

### validity_date

**Status:** PASS

```text
{'status': 'PASS', 'details': {'transaction_date': {'invalid_dates': 0, 'future_dates': 0, 'future_dates_allowed': False}}, 'failed_columns': []}
```

### consistency

**Status:** PASS

```text
{'status': 'PASS', 'details': [{'rule': {'type': 'non_negative', 'column': 'amount'}, 'status': 'PASS', 'violations': 0, 'description': 'amount >= 0: 0 violations'}], 'failed_rules': []}
```

### referential_integrity

**Status:** PASS

```text
{'status': 'PASS', 'details': {'project_id': {'reference': 'projects.project_id', 'invalid_count': 0, 'invalid_examples': []}, 'approved_by': {'reference': 'employees.employee_id', 'invalid_count': 0, 'invalid_examples': []}}, 'failed_columns': []}
```

### distribution

**Status:** FAIL

```text
{'status': 'FAIL', 'details': {'payment_status': {'top_value': 'Paid', 'top_share': 0.7512, 'threshold': 0.3}, 'category': {'top_value': 'Software', 'top_share': 0.2032, 'threshold': 0.3}, 'currency': {'top_value': 'AED', 'top_share': 1.0, 'threshold': 0.3}}, 'failed_columns': ['payment_status', 'currency']}
```

### freshness

**Status:** PASS

```text
{'status': 'PASS', 'details': {'column': 'transaction_date', 'latest_date': '2026-08-04', 'age_days': 21, 'maximum_allowed_age_days': 30}}
```

### outliers

**Status:** FAIL

```text
{'status': 'FAIL', 'details': {'amount': {'mean': 62848.38, 'std_dev': 118166.86, 'outlier_count': 1593}}, 'failed_columns': ['amount']}
```
