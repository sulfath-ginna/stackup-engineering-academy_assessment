# Data Governance Document

**StackUp Engineering Academy — Pillar 4, Task 4.2**  
**Prepared for:** Presight project management analytics platform  
**Scope:** `projects.csv`, `employees.csv`, `transactions.json`, `employees_salary_history.csv`

> **Governance note:** Retention periods below are a practical enterprise policy for this assessment. UAE legal requirements can vary by record type, entity, free-zone status, and tax status. The policy therefore applies the longest relevant business/regulatory period identified and should be validated by Legal/Compliance before production rollout.

---

## 1. Data Inventory

| Dataset | Source system | Format | Update frequency | Current volume estimate | Daily growth |
|---|---|---:|---|---:|---|
| projects | Project Management System | CSV | Daily / when project master changes | 500 rows | Variable; monitor ingestion |
| employees | HR Information System (HRIS) | CSV | Daily / when employee master changes | 1,000 rows | Variable; monitor ingestion |
| transactions | Finance / ERP / Accounts Payable | JSON | Daily / near-real-time batch | 50,000 rows | Variable; expected highest-growth dataset |
| employees_salary_history | HRIS / Payroll | CSV | Event-driven on compensation or role change | 1,826 rows | Low and event-driven |

### Inventory observations

- `projects` is master/reference data used for project analytics and transaction enrichment.
- `employees` contains workforce master data and directly identifiable personal information.
- `transactions` is financial activity data and contains employee identifiers through `approved_by`.
- `employees_salary_history` contains historical compensation and role changes and is the most restricted HR dataset in this scope.
- Update frequency and daily growth are operational assumptions for the assessment and should be replaced with measured production values once ingestion monitoring is available.

---

## 2. Data Classification

### Classification definitions

| Classification | Definition |
|---|---|
| **Public** | Non-sensitive information that may be externally shareable after approval. |
| **Internal** | Information intended for internal business use with no direct personal or highly sensitive content. |
| **Confidential** | Sensitive business, commercial, financial, operational, or security-related information requiring restricted access. |
| **Personal (PII)** | Information that directly identifies, or can reasonably be linked to, an individual. Privacy and data-protection controls apply. |

### Regulation notation for PII

For this assessment, PII is governed primarily by the **UAE Personal Data Protection Law (Federal Decree-Law No. 45 of 2021)**.

**GDPR** may also apply where its territorial scope is triggered, for example where processing relates to an EU/EEA establishment or qualifying offering/monitoring of individuals in the EU/EEA.

Accordingly, PII fields below are marked:

**UAE PDPL + GDPR where applicable.**

---

### 2.1 `projects.csv`

| Column | Classification | Regulation / rationale |
|---|---|---|
| `project_id` | Internal | Internal project identifier. |
| `project_name` | Internal | Internal project/business information. |
| `department` | Internal | Organisational structure information. |
| `status` | Internal | Operational project status. |
| `start_date` | Internal | Project schedule information. |
| `end_date` | Internal | Project schedule information. |
| `budget` | Confidential | Commercial and financial information. |
| `actual_cost` | Confidential | Commercial and financial performance information. |
| `project_manager_id` | Personal (PII) | Employee-linked identifier. UAE PDPL + GDPR where applicable. |
| `priority` | Internal | Operational prioritisation. |
| `region` | Internal | Business or operational geography. |

---

### 2.2 `employees.csv`

| Column | Classification | Regulation / rationale |
|---|---|---|
| `employee_id` | Personal (PII) | Unique employee identifier. UAE PDPL + GDPR where applicable. |
| `full_name` | Personal (PII) | Directly identifies a person. UAE PDPL + GDPR where applicable. |
| `email` | Personal (PII) | Direct employee contact identifier. UAE PDPL + GDPR where applicable. |
| `department` | Personal (PII) | Employment information linked to an identifiable employee. UAE PDPL + GDPR where applicable. |
| `role` | Personal (PII) | Employment/profile information linked to an identifiable employee. UAE PDPL + GDPR where applicable. |
| `level` | Personal (PII) | Employment grade linked to an identifiable employee. UAE PDPL + GDPR where applicable. |
| `hire_date` | Personal (PII) | Employment history information. UAE PDPL + GDPR where applicable. |
| `salary` | Personal (PII) | Sensitive compensation information linked to an identifiable employee. UAE PDPL + GDPR where applicable. |
| `manager_id` | Personal (PII) | Identifies reporting relationships between employees. UAE PDPL + GDPR where applicable. |
| `region` | Personal (PII) | Employment location/region linked to an identifiable employee. UAE PDPL + GDPR where applicable. |
| `status` | Personal (PII) | Employment status linked to an identifiable employee. UAE PDPL + GDPR where applicable. |
| `years_experience` | Personal (PII) | Professional history/profile information. UAE PDPL + GDPR where applicable. |

---

### 2.3 `transactions.json`

| Column | Classification | Regulation / rationale |
|---|---|---|
| `transaction_id` | Confidential | Unique finance transaction identifier. |
| `project_id` | Internal | Links finance activity to an internal project. |
| `vendor_id` | Confidential | Supplier/business identifier used in finance operations. |
| `vendor_name` | Confidential | Commercial counterparty information. |
| `category` | Internal | Transaction/business expense category. |
| `amount` | Confidential | Financial value. |
| `currency` | Internal | Currency code. |
| `transaction_date` | Confidential | Financial transaction timing. |
| `approved_by` | Personal (PII) | Employee identifier revealing an individual's approval activity. UAE PDPL + GDPR where applicable. |
| `payment_status` | Confidential | Financial process/status information. |
| `invoice_ref` | Confidential | Finance/document reference that may expose commercial relationships. |
| `notes` | Confidential | Free-text business data; may accidentally contain PII and should be treated as restricted. |

### Free-text control

The `notes` field should be monitored for unexpected personal information, credentials, bank details, or other sensitive content before broad analytical use.

---

### 2.4 `employees_salary_history.csv`

| Column | Classification | Regulation / rationale |
|---|---|---|
| `employee_id` | Personal (PII) | Employee identifier. UAE PDPL + GDPR where applicable. |
| `previous_salary` | Personal (PII) | Historical compensation. UAE PDPL + GDPR where applicable. |
| `new_salary` | Personal (PII) | Current/new compensation at the effective event. UAE PDPL + GDPR where applicable. |
| `previous_role` | Personal (PII) | Historical employment profile. UAE PDPL + GDPR where applicable. |
| `new_role` | Personal (PII) | Employment profile. UAE PDPL + GDPR where applicable. |
| `previous_level` | Personal (PII) | Historical employee grade. UAE PDPL + GDPR where applicable. |
| `new_level` | Personal (PII) | New employee grade. UAE PDPL + GDPR where applicable. |
| `effective_date` | Personal (PII) | Date of an employment or compensation change linked to a person. UAE PDPL + GDPR where applicable. |
| `change_type` | Personal (PII) | Employment event information linked to a person. UAE PDPL + GDPR where applicable. |
| `change_reason` | Personal (PII) | Employment/HR decision information that may be particularly sensitive. UAE PDPL + GDPR where applicable. |

---

### Classification handling rules

- **Public:** standard approved publication controls.
- **Internal:** authenticated employee access; no anonymous external exposure.
- **Confidential:** role-based access, encryption at rest and in transit, audit logging, restricted export.
- **PII:** all Confidential controls plus privacy-purpose limitation, minimisation, controlled sharing, deletion/anonymisation processes, and data-subject-rights support where applicable.

---

## 3. Data Ownership

| Dataset | Data Owner (role) | Data Steward (role) | Access approver |
|---|---|---|---|
| projects | Head of Project Management / PMO Director | Project Data Steward / PMO Analyst | PMO Director or delegated system owner |
| employees | HR Director | HR Data Steward / HRIS Lead | HR Director |
| transactions | Finance Director / CFO delegate | Finance Data Steward / Financial Systems Lead | Finance Director / Controller |
| employees_salary_history | HR Director / Compensation & Benefits Head | HRIS / Compensation Data Steward | HR Director plus Compensation & Benefits Head |

### Owner vs. Steward

The **Data Owner** is accountable for the business use, risk, access policy, retention decision, and acceptable quality level of a dataset.

The **Data Steward** manages the data operationally. The steward monitors definitions and quality, coordinates corrections, maintains metadata, supports lineage, and ensures that day-to-day handling follows the owner's governance policy.

In simple terms:

**The Owner is accountable for the data; the Steward looks after it.**

---

## 4. Retention Policy

| Dataset | Retention policy | Justification | Disposal method | Policy enforcer |
|---|---|---|---|---|
| projects | 7 years after project financial closure, unless a longer litigation or contractual hold applies | Project records support financial reporting, audit, contracts, and project history. Records supporting tax/accounting positions may need longer retention. | Archive during retention; securely delete or anonymise after expiry and hold review | PMO Data Owner + Records Management + Legal/Finance |
| employees | At least 2 years after end of service; operational policy may retain necessary payroll, audit, or claims records longer | UAE labour requirements support retaining worker records after employment. Additional business or financial requirements may justify longer retention for specific fields. | Delete personal records no longer required; anonymise records retained for statistics; preserve only legally required evidence | HR Director + HRIS + Legal/Records Management |
| transactions | 7 years after the end of the relevant financial/tax period, or longer if subject to investigation or legal hold | Transactions are accounting and finance evidence and may be required for tax, audit, investigation, or contractual purposes. | Secure deletion after retention and hold review; archive immutable audit evidence during retention | Finance Director + Tax/Compliance + Records Management |
| employees_salary_history | 7 years after the relevant financial/tax or termination-related event, using the longer applicable retention period | Salary history supports payroll, accounting, audit, employment claims, promotion history, and compensation governance. | Restricted archive; securely delete at expiry; anonymise if only statistical history is still needed | HR Director + Compensation & Benefits + Finance/Tax + Legal |

---

### Salary-history special consideration

`employees_salary_history.csv` should **not** be treated like ordinary low-risk employee master data.

It contains:

- historical salary values;
- promotions and role changes;
- employee levels;
- effective dates;
- reasons for compensation or employment changes.

Controls should include:

1. access restricted primarily to authorised HR and Compensation personnel;
2. encryption at rest and in transit;
3. audit logs for reads, exports, and changes;
4. no salary-level data in general BI dashboards unless aggregated or anonymised;
5. retention based on the longest applicable legal/business requirement;
6. legal-hold capability for employment disputes, investigations, audit, or litigation.

---

## 5. Access Control

Access levels:

- `None`
- `Read`
- `Read + Write`
- `Full (including delete)`

| Persona | Projects | Employees | Transactions | Salary History |
|---|---|---|---|---|
| Data Engineer | Read + Write | Read | Read + Write | None |
| BI Analyst | Read | Read (masked/minimised view) | Read | None |
| Finance Team | Read | Read (limited employee identity fields only) | Read + Write | None |
| HR Team | Read | Read + Write | None | Read + Write |
| Executive | Read | Read (aggregated view) | Read (aggregated view) | None |

### Access-control rationale

#### Data Engineer

- Needs technical access to ingest, transform, validate, and recover pipelines.
- Does not need routine access to individual salary history.
- Production delete rights should be separated from normal engineering duties.

#### BI Analyst

- Needs analytics-ready project and financial data.
- Employee access should use a minimised or masked view.
- Salary history is not required for ordinary business intelligence.

#### Finance Team

- Requires transaction maintenance and project financial context.
- Employee information should be limited to what is required for approval or accounting.
- Historical individual salary changes should normally remain under HR control.

#### HR Team

- Owns employee and compensation processes.
- Does not require general transaction data for normal HR activities.

#### Executive

- Should generally receive aggregated dashboards rather than row-level personal or compensation data.

---

### Additional technical controls

- Role-Based Access Control (RBAC).
- Multi-Factor Authentication for privileged users.
- Encryption in transit and at rest.
- Centralised audit logs.
- Quarterly access recertification for PII and Confidential datasets.
- Separate privileged role for deletion.
- No production data copied to developer laptops unless specifically approved.
- Mask employee email, salary, and IDs in non-production environments.
- Monitor bulk exports of employee and financial records.

---

## 6. Data Lineage

```mermaid
flowchart TD
    A[Project Management System] --> PRAW[Raw projects.csv]
    B[HR Information System] --> ERAW[Raw employees.csv]
    B --> SRAW[Raw employees_salary_history.csv]
    C[Finance / ERP] --> TRAW[Raw transactions.json]

    PRAW --> PDQ[Project DQ Checks]
    ERAW --> EDQ[Employee DQ Checks]
    SRAW --> SDQ[Salary History DQ Checks]
    TRAW --> TDQ[Transaction DQ Checks]

    PDQ --> PCLN[projects_clean.csv]
    EDQ --> ECLN[employees_clean.csv]
    TDQ --> TENR[Transaction Transform + Enrichment]

    PCLN --> TENR
    ECLN --> TENR
    TENR --> TCLN[transactions_clean.csv]

    PCLN --> WH[Analytics Warehouse / DuckDB]
    ECLN --> WH
    TCLN --> WH
    SDQ --> WH

    WH --> SQL[Business SQL / Aggregations]
    SQL --> BI[BI Dashboard / Management Reporting]