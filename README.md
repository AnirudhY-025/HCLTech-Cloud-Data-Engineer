# HCLTech Cloud Data Engineer Lab Assessment

Repository containing practical tasks, scripts, and documentation for the Cloud Data Engineer assessment.

**Candidate:** Yekkaluri Anirudh  
**Roll Number:** 23BTRCL241  
**Track:** Cloud Engineering & Big Data Analytics  

---

## Overview

This repository documents the implementation of database design, systems administration, big data pipelines, and cloud workflows across six sections:

- **Section A: Advanced SQL** - Schema creation, multi-table joins, correlated subqueries, ranking window functions, and query optimization.
- **Section B: Linux** - System log analysis using grep/awk, POSIX permissions, server monitoring, and an automated backup bash script.
- **Section C: PySpark Data Engineering** - DataFrame operations, data cleaning, deduplication, handling missing values, and join performance.
- **Section D: AWS Cloud Services** - S3 bucket security, least-privilege IAM policies, EC2 health checks, CloudWatch alarms, and event-driven architecture.
- **Section E: AWS Step Functions** - Serverless state machine coordination, ASL definition, error retry/catch handling, and execution tracing.
- **Section F: Microsoft Azure** - Enterprise cloud theory covering ADLS Gen2, Azure Data Factory, Databricks with Delta Lake, Synapse Analytics, Entra ID, and Key Vault.

---

## Section C: PySpark Notebook

All PySpark code, data cleaning pipelines, and join operations for Questions 7, 8, and 9 were written and executed in Google Colab:

- **Colab Notebook:** [https://colab.research.google.com/drive/1b9TJR8Fbx1R5jrjVk9eCwy06X17CEBoF](https://colab.research.google.com/drive/1b9TJR8Fbx1R5jrjVk9eCwy06X17CEBoF)

---

## Detailed Task Breakdown

### Section A: Advanced SQL
- **Question 1 (EnterpriseDB Design):**
  - Created EnterpriseDB with Employees and Departments tables with primary and foreign keys.
  - Inserted 15+ employee records and 5 departments (including an unassigned department).
  - Joins: Displayed employee, department, and manager names using inner and self joins.
  - Subqueries: Filtered employees earning more than their department average salary.
  - Ranking: Used `DENSE_RANK() OVER (PARTITION BY DepartmentID ORDER BY Salary DESC)` to get top 3 employees per department, retaining ties.
  - Found second-highest distinct salary using nested scalar subquery without `LIMIT` or `TOP`.
- **Question 2 (Sales Analytics & Indexing):**
  - Created Sales table and calculated total revenue per product.
  - Calculated monthly revenue and month-over-month revenue change using `LAG()` window function.
  - Rewrote `WHERE YEAR(JoiningDate) = 2025` to a date range `JoiningDate >= '2025-01-01' AND JoiningDate < '2026-01-01'` to enable B-tree index range scanning.
- **Question 3 (SQL Troubleshooting):**
  - Analyzed execution plans with `EXPLAIN` to identify full table scans (`type: ALL`) and filesorts.
  - Designed composite index on `(DepartmentID, Salary)` placing equality column first.
  - Identified duplicate customer emails using `GROUP BY Email HAVING COUNT(*) > 1`.

### Section B: Linux
- **Question 4 (Log Analysis):**
  - Filtered all ERROR entries from `/var/log/application.log` using `grep`.
  - Displayed the 20 most recent errors using `grep 'ERROR' file | tail -20`.
  - Counted total error entries with `grep -c 'ERROR' file`.
  - Extracted errors for a specific date into a new file using output redirection.
  - Calculated hourly error counts using `awk '{print substr($2, 1, 2)}' | sort | uniq -c`.
- **Question 5 (Permissions & Diagnostics):**
  - Created group `finance` and report file using `groupadd` and `touch`.
  - Set permissions to `640` (`rw-r-----`) with `chown root:finance` and `chmod 640`.
  - Monitored top CPU processes using `top` and `ps aux --sort=-%cpu`.
  - Checked memory and disk space using `free -h` and `df -h`.
  - Located files larger than 100 MB using `find /opt/app -type f -size +100M`.
- **Question 6 (Bash Scripting):**
  - Authored `backup_check.sh` accepting source and destination arguments.
  - Validated parameter count, verified source existence, created destination via `mkdir -p`.
  - Created timestamped archive using `tar -czf`.
  - Handled success and failure exit codes.

### Section C: PySpark Data Engineering
- **Question 7 (DataFrame Operations):**
  - Created 15-record employee DataFrame and displayed schema and first 5 rows.
  - Filtered employees with salary > 60,000.
  - Added derived `AnnualSalary` column (`col('Salary') * 12`).
  - Grouped by department to calculate average salary and headcount.
  - Displayed top 5 highest earners using `orderBy(col('Salary').desc()).show(5)`.
- **Question 8 (Data Cleaning & Aggregation):**
  - Ingested `transactions.csv` using explicit `StructType` schema.
  - Removed duplicate records by `TransactionID` using `dropDuplicates()`.
  - Handled missing values: dropped rows missing critical IDs (`TransactionID`, `CustomerID`), filled numeric nulls with `0`.
  - Cast `TransactionDate` to `DateType` via `to_date()` and removed invalid dates.
  - Calculated revenue (`Quantity * UnitPrice`) and aggregated revenue by product in descending order.
- **Question 9 (Joins & Spark Internals):**
  - Joined Customers and Orders DataFrames on `CustomerID`.
  - Found customers with no orders using a `left_anti` join.
  - Aggregated total order amount by city.
  - Evaluated broadcast join use cases (tables < 10 MB broadcast to worker memory, avoiding shuffle).
  - Clarified lazy transformations (`filter`, `select`) versus trigger actions (`count`, `show`).

### Section D: AWS Cloud Services
- **Question 10 (S3 & IAM Security):**
  - Created private S3 bucket with Block Public Access enabled and uploaded sample report.
  - Wrote least-privilege IAM policy restricting read access exclusively to target bucket ARN.
  - Documented access restriction testing using CLI (403 Access Denied) and IAM Policy Simulator.
  - Detailed server-side encryption at rest using AWS KMS / SSE-S3.
- **Question 11 (EC2 & CloudWatch):**
  - Verified instance state and 2/2 status checks in EC2 console.
  - Analyzed impact of Security Groups (instance firewall) and Route Tables (subnet routing).
  - Configured CloudWatch Alarm on `CPUUtilization > 85%` with SNS email alert.
  - Investigated CPU spikes using SSH and `top` / `ps aux`.
- **Question 12 (Event-Driven Architecture):**
  - Selected AWS Lambda for serverless, event-driven processing.
  - Configured S3 `s3:ObjectCreated:*` event notification to trigger Lambda function.
  - Outlined IAM execution role permissions for S3 read/write and CloudWatch logging.
  - Designed error capture using CloudWatch Logs and SQS Dead-Letter Queues.
  - Documented prevention of recursive triggering by separating input/output buckets.

### Section E: AWS Step Functions
- **Question 13 (State Machine Design):**
  - Orchestrated 5-stage ETL pipeline: ValidateFile, Choice check, TransformData, CalculateSales, StoreResults, SendNotification.
  - Used Choice state for conditional branching on validation result.
  - Implemented Retry with exponential backoff for transient errors and Catch for permanent errors.
  - Configured service integrations using task Resource ARNs.
  - Used execution history console to debug failed states with input/output payloads.
- **Question 14 (Implementation & Execution):**
  - Submitted full ASL state machine definition.
  - Verified successful execution run (all green states).
  - Verified error handling path on invalid file input.

### Section F: Microsoft Azure
- **Question 15 (Storage & ADF):**
  - Azure Blob Storage (flat namespace) vs ADLS Gen2 (hierarchical namespace, POSIX ACLs).
  - Azure Data Factory pipeline architecture (Activities, Linked Services, triggers).
  - Azure Integration Runtime (cloud compute) vs Self-hosted IR (on-prem connectivity).
- **Question 16 (Databricks & Synapse):**
  - Databricks workspace and cluster configuration with auto-termination.
  - Delta Lake ACID guarantees, schema enforcement, time travel, and upsert operations.
  - Azure Synapse (SQL data warehousing & BI) vs Databricks (Spark ML & feature engineering).
- **Question 17 (Security & Monitoring):**
  - Microsoft Entra ID (Azure AD) authentication, MFA, and RBAC authorization.
  - Azure Monitor platform metrics and Application Insights APM diagnostics.
  - Azure Key Vault centralized secret storage with Managed Identity authentication.

---

## Submission Artifacts

- **Report Document:** `HCL_CloudLab_Assessment_Report.docx` (all 49 execution screenshots and mapped tasks)
- **PySpark Colab Notebook:** [Google Colab Link](https://colab.research.google.com/drive/1b9TJR8Fbx1R5jrjVk9eCwy06X17CEBoF)
