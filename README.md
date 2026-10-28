# HCLTech-Cloud-Data-Engineer
Hands-On Practical and Clear Explanations of Beginner Level Tasks of an Cloud Data Engineer

# Repository Overview

This repository contains the complete implementation, automation scripts, analytical queries, architectural designs, and verification artifacts for the HCL CloudLab Assessment. The project covers end-to-end data engineering and cloud infrastructure workflows across six core modules:

Section A: Advanced SQL & Optimization — Enterprise relational database design, complex analytical queries, window functions (LAG, DENSE_RANK), and SARGable index optimization.
Section B: Linux Administration & Shell Scripting — High-volume log analytics (grep, awk), secure POSIX permissions (chmod 640), system troubleshooting, and an automated backup bash script (backup_check.sh).
Section C: PySpark Data Engineering — Distributed DataFrame processing, data deduplication, missing value imputation, relational joins, broadcast join optimizations, and PySpark internals.
Live Interactive Notebook: Google Colab PySpark Implementation
Section D: AWS Cloud Services — Secure S3 bucket architecture with Block Public Access, least-privilege IAM policies, EC2 health checks, CloudWatch alarms with SNS alerting, and serverless event-driven architectures.
Section E: AWS Step Functions — Serverless state machine orchestration in Amazon States Language (ASL), dynamic Choice branching, exponential backoff Retry policies, and Catch fault tolerance.
Section F: Microsoft Azure Enterprise Cloud — Big data storage (ADLS Gen2 vs Blob), Azure Data Factory (ADF) ETL pipelines, Databricks with Delta Lake ACID guarantees, Azure Synapse Analytics, Microsoft Entra ID (RBAC), and Azure Key Vault.
📂 Repository Structure
text
├── sql/
│   ├── 01_enterprise_db_setup.sql      # Schema definition (Employees & Departments) + 15+ records
│   ├── 02_advanced_queries.sql         # Joins, subqueries, 2nd highest salary, DENSE_RANK()
│   ├── 03_sales_analytics.sql          # Sales revenue aggregation & LAG() window function
│   └── 04_query_optimization.sql      # SARGable query rewrite & EXPLAIN execution plans
├── linux/
│   ├── log_analysis_commands.sh        # grep/awk log processing pipelines
│   ├── permissions_and_checks.sh       # chown/chmod 640, top, free, df, find >100M
│   └── backup_check.sh                 # Production-grade automated directory backup script
├── pyspark/
│   ├── pyspark_data_engineering.ipynb  # Jupyter/Colab notebook (all Q7, Q8, Q9 tasks)
│   └── colab_link.txt                  # Link to live interactive Colab environment
├── aws/
│   ├── iam_least_privilege_policy.json # Restrictive S3 read IAM policy definition
│   ├── event_driven_architecture.md    # S3 -> Lambda -> SQS architectural design
│   └── ec2_cloudwatch_alarm_config.md  # Metric alarm setup (CPUUtilization > 85%)
├── step_functions/
│   ├── sales_processing_workflow.asl.json # Amazon States Language state machine definition
│   └── state_machine_architecture.md   # Choice branching & Retry/Catch documentation
├── azure/
│   └── azure_cloud_architecture.md     # ADLS Gen2, ADF, Databricks, Synapse, Entra ID, Key Vault
└── README.md                           # Master repository documentation

🛠️ Module Breakdown & Implementation Details
Section A: Advanced SQL (EnterpriseDB & Sales Analytics)
Schema Design & Referential Integrity: Created EnterpriseDB with Employees and Departments tables enforcing primary key and foreign key constraints, populating 15+ employee records across multiple management hierarchies and an unassigned department.
Complex Joins & Subqueries:
Self-joins (LEFT JOIN Employees m ON e.ManagerID = m.EmployeeID) to display hierarchical employee-manager relationships.
Correlated subqueries comparing individual salaries against dynamic departmental averages (WHERE e.Salary > (SELECT AVG(Salary)...)).
Portably retrieved the 2nd highest distinct salary without vendor-specific clauses (LIMIT or TOP) using nested scalar subqueries.
Window Functions: Applied DENSE_RANK() OVER (PARTITION BY DepartmentID ORDER BY Salary DESC) to identify top 3 earners per department while retaining ties at the cutoff.
Query Optimization (SARGability): Rewrote non-SARGable query WHERE YEAR(JoiningDate) = 2025 into a direct date range scan (WHERE JoiningDate >= '2025-01-01' AND JoiningDate < '2026-01-01'), enabling the query optimizer to utilize B-tree indexes rather than performing expensive full table scans (type: ALL).
Execution Plan Diagnostics: Analyzed EXPLAIN outputs to identify full table scans, row scan estimates, and file sorts, designing a composite index on (DepartmentID, Salary) with leading equality columns.
Section B: Linux System Administration & Shell Scripting
High-Volume Log Analysis:
Filtered ERROR events from /var/log/application.log.
Piped to tail -20 for real-time monitoring of recent failures.
Rapid count evaluation using grep -c.
Extracted specific dates to dedicated archival files using shell redirection.
Hourly aggregation using awk '{print substr($2, 1, 2)}' | sort | uniq -c.
Security & System Troubleshooting:
Created finance system group and assigned strict chmod 640 permissions (rw-r-----) to ensure owner read/write, group read-only, and zero permissions for other users.
System monitoring commands: top/ps aux for CPU hogs, free -h for memory, df -h for disk utilization, and find /opt/app -type f -size +100M for high-volume files.
Automated Backup Script (backup_check.sh):
Robust argument validation ($# -lt 2).
Source existence verification ([ ! -d "$SRC" ]).
Automatic destination creation (mkdir -p "$DST").
Timestamped compression using tar -czf "$DST/backup_$(date +%Y%m%d_%H%M%S).tar.gz" -C "$SRC" ..
Clean exit codes (0 for success, 1 for error handling).
Section C: PySpark Data Engineering

🚀 Live Interactive Colab Notebook: Click here to view code & cell outputs

DataFrame Operations (Q7): Created distributed employee DataFrame with explicit types, demonstrated schema introspection (printSchema()), conditional filtering (Salary > 60000), derived column projections (withColumn('AnnualSalary', col('Salary') * 12)), departmental aggregations (groupBy().agg(avg, count)), and sorted limit actions (orderBy().show(5)).
Data Cleaning & Deduplication (Q8):
Ingested messy transaction records using explicit StructType schema.
Deduplicated records based on primary business key (dropDuplicates(['TransactionID'])).
Missing value strategy: dropped unrecoverable primary key nulls (dropna(subset=['TransactionID', 'CustomerID'])) and imputed measurement nulls with default zeros (fillna({'Quantity': 0, 'UnitPrice': 0.0})).
Formatted dates with to_date() and removed invalid date rows.
Product revenue aggregation sorted descending.
Relational Joins & Execution Concepts (Q9):
Inner join combining Orders and Customers.
Left anti join (orders_df.join(..., how='left_anti')) to isolate customers with zero orders.
Geographic revenue aggregation grouped by city.
Comprehensive analysis of Broadcast Joins (eliminating cluster network shuffle by copying small tables < 10 MB to all executors) and Transformations vs. Actions (lazy DAG compilation vs. triggered execution).
Section D: AWS Cloud Services & Event-Driven Architecture
S3 Security & IAM Least Privilege: Created private S3 bucket with all four Block Public Access settings enabled. Authored strict IAM policy granting s3:GetObject and s3:ListBucket restricted exclusively to arn:aws:s3:::hr-employee-reports/*.
Verification & Encryption: Validated security via AWS CLI simulated access (403 AccessDenied) and IAM Policy Simulator. Documented envelope encryption at rest using AWS KMS / SSE-S3.
EC2 Troubleshooting & CloudWatch: Monitored instance health checks (2/2 status checks passed), analyzed network reachability differences between stateful Security Groups and subnet Route Tables, configured CloudWatch Alarm for CPUUtilization > 85% with SNS email dispatch, and detailed process isolation via SSH top.
Event-Driven Architecture: Designed automated ingestion pipeline: S3 Upload (s3:ObjectCreated:*) → AWS Lambda execution → processed output to destination S3 bucket. Handled idempotency via DynamoDB tracking and failure auditing via SQS Dead-Letter Queues (DLQ).
Section E: AWS Step Functions State Machine
End-to-End Orchestration: Designed serverless workflow coordinating:
ValidateFile (Lambda task)
CheckValidity (Choice state branching)
TransformData (ETL transform)
CalculateSales (Product-level aggregations)
StoreResults (Direct S3 SDK putObject)
SendNotification (Amazon SNS notification)
Fault Tolerance & Resilience: Implemented exponential backoff Retry policies for transient timeouts (IntervalSeconds: 3, MaxAttempts: 3, BackoffRate: 2) and fallback Catch handlers routing unrecoverable errors to FileInvalid terminal state.
Execution Monitoring: Verified state transitions and JSON payload flows using AWS Step Functions Execution History console for both successful runs and caught failure paths.
Section F: Microsoft Azure Enterprise Cloud
Storage Architecture: Architectural comparison between Azure Blob Storage (flat namespace for unstructured objects) and Azure Data Lake Storage Gen2 (hierarchical directory namespace, POSIX ACLs, optimized for Synapse/Databricks).
ETL Pipeline Orchestration: Azure Data Factory (ADF) pipelines, Activities, Linked Services, triggers, and the distinction between cloud Azure Integration Runtime (IR) and on-premises Self-hosted IR.
Databricks & Delta Lake: Workspace deployment, compute cluster auto-termination cost controls, and Delta Lake ACID transaction guarantees, schema enforcement, time-travel versioning, and UPSERT operations.
Security & Monitoring: Centralized cloud identity and RBAC via Microsoft Entra ID (Azure AD), telemetry collection through Azure Monitor and Application Insights, and secure secrets management using Azure Key Vault with Managed Identities.
