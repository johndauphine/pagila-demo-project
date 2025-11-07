# Documentation Index - Pagila Replication Pipeline

## Quick Navigation

### For New Users: Getting Started
**Start Here:** [`CLAUDE.md`](../CLAUDE.md) - Quick Start Section (5 steps to running environment)

### For Developers: Complete Setup
**Reference:** [`README.md`](../README.md) - Comprehensive user guide with detailed explanations

### For AI Agents: Contributing
**Guidelines:** [`AGENTS.md`](../AGENTS.md) - Repository structure, coding standards, build commands

### For SQL Server Migration: Technical Details
**Deep Dive:** [`MSSQL_MIGRATION.md`](../MSSQL_MIGRATION.md) - Complete migration guide and troubleshooting

---

## Documentation by Use Case

### Use Case 1: "I want to run this project right now"
```
1. Read: CLAUDE.md → Quick Start
2. Execute: Steps 1-5 (choose Postgres OR SQL Server)
3. Verify: Step 5 verification command
```

### Use Case 2: "I want to understand the architecture"
```
1. Read: README.md → Section 7 (Architecture Overview)
2. Read: CLAUDE.md → DAGs Structure
3. Read: MSSQL_MIGRATION.md → Data Flow (if using SQL Server)
```

### Use Case 3: "I want to contribute or modify code"
```
1. Read: AGENTS.md → All sections
2. Read: README.md → Section 14 (Contributor Notes)
3. Review: tests/dags/test_dag_example.py
```

### Use Case 4: "I want to switch from Postgres to SQL Server target"
```
1. Read: README.md → Section 13 (SQL Server Configuration)
2. Read: MSSQL_MIGRATION.md → Switching Instructions
3. Apply: Section 13.5 step-by-step changes
```

### Use Case 5: "I'm troubleshooting SQL Server errors"
```
1. Read: MSSQL_MIGRATION.md → Section 4 (Technical Challenges)
2. Read: README.md → Section 13.6 (Troubleshooting)
3. Check: Airflow logs with error messages
```

### Use Case 6: "I need to understand type conversions"
```
1. Read: CLAUDE.md → SQL Server Type Conversions table
2. Read: MSSQL_MIGRATION.md → Section 2.2 (Schema Conversion)
3. Review: include/pagila/schema_mssql.sql
```

---

## File Summary

| File | Lines | Size | Purpose | Update Frequency |
|------|-------|------|---------|-----------------|
| `README.md` | 712 | 25K | Primary user documentation | Medium |
| `CLAUDE.md` | 325 | 12K | AI assistant quick reference | Medium |
| `MSSQL_MIGRATION.md` | 298 | 11K | SQL Server migration guide | Low |
| `AGENTS.md` | 75 | 4.7K | Repository guidelines | Low |

---

## Documentation Coverage

### Infrastructure Setup
- ✓ Docker container commands (all environment variables)
- ✓ Network setup (auto-discovery script)
- ✓ Airflow connections (all parameters)
- ✓ Package dependencies (both Postgres and SQL Server)

### Data Pipeline
- ✓ Source loading DAG (idempotent hashing)
- ✓ Replication DAG (memory-capped streaming)
- ✓ Postgres target (COPY TO/FROM)
- ✓ SQL Server target (CSV bulk loading)

### Schema Management
- ✓ Postgres schema (include/pagila/schema.sql)
- ✓ SQL Server schema (include/pagila/schema_mssql.sql)
- ✓ Type conversion reference
- ✓ Auto-increment handling (both databases)

### Validation & Testing
- ✓ Verification commands (both targets)
- ✓ Expected row counts (15 tables)
- ✓ DAG test commands
- ✓ Pytest suite documentation

### Troubleshooting
- ✓ Common errors with solutions
- ✓ Database-specific quirks
- ✓ Performance tuning guidance
- ✓ Debugging commands

---

## Cross-References

### README.md References
- Section 5.2 → `schema_mssql.sql`
- Section 9.2 → SQL Server verification
- Section 11 → File layout
- Section 13 → `MSSQL_MIGRATION.md`
- Section 14 → `AGENTS.md`

### CLAUDE.md References
- Quick Start → Database setup commands
- Technology Stack → Type conversions
- Key Files → `MSSQL_MIGRATION.md`
- Verification → Expected row counts

### AGENTS.md References
- Project Structure → `schema_mssql.sql`, `MSSQL_MIGRATION.md`
- Build Commands → Database containers
- SQL Server Notes → Key technical details

### MSSQL_MIGRATION.md References
- Section 2.2 → Type conversion table
- Section 4 → Error solutions
- Section 5 → Performance characteristics
- Section 9 → References to external docs

---

## Search Keywords by Topic

### Docker Setup
- Files: `README.md` (Section 3), `CLAUDE.md` (Quick Start), `AGENTS.md` (Build Commands)
- Keywords: `docker run`, `pagila-pg-source`, `pagila-mssql-target`, `network connect`

### SQL Server
- Files: `MSSQL_MIGRATION.md` (all), `README.md` (Section 13), `CLAUDE.md` (Type Conversions)
- Keywords: `Azure SQL Edge`, `pymssql`, `DBCC CHECKIDENT`, `DELETE FROM`, `schema_mssql.sql`

### Airflow Connections
- Files: `README.md` (Section 4), `CLAUDE.md` (Quick Start Step 3), `AGENTS.md`
- Keywords: `pagila_postgres`, `pagila_tgt`, `pagila_mssql`, `connections add`

### Data Replication
- Files: `README.md` (Section 7), `CLAUDE.md` (DAGs Structure), `MSSQL_MIGRATION.md` (Section 3.3)
- Keywords: `SpooledTemporaryFile`, `COPY`, `CSV`, `batch INSERT`, `128MB`

### Troubleshooting
- Files: `MSSQL_MIGRATION.md` (Section 4, 9), `README.md` (Section 13.6)
- Keywords: `TRUNCATE error`, `GO statement`, `autocommit`, `binary mode`, `foreign key`

---

## Maintenance Notes

### When to Update Documentation

**Update README.md when:**
- Adding new target database types
- Changing connection configuration
- Modifying DAG structure
- Adding new verification queries

**Update CLAUDE.md when:**
- Changing quick start steps
- Adding new commands to workflow
- Updating technology stack
- Adding new verification methods

**Update AGENTS.md when:**
- Changing repository structure
- Updating coding standards
- Adding new build commands
- Changing test requirements

**Update MSSQL_MIGRATION.md when:**
- Discovering new SQL Server issues
- Adding troubleshooting solutions
- Updating type conversions
- Changing data flow architecture

### Version History
- Initial version: November 6, 2025
- SQL Server support added: November 6, 2025
- Documentation complete: November 6, 2025

---

## External References

### Official Documentation
- [Apache Airflow](https://airflow.apache.org/docs/)
- [Astronomer Astro CLI](https://docs.astronomer.io/astro/cli/overview)
- [PostgreSQL 16](https://www.postgresql.org/docs/16/)
- [Azure SQL Edge](https://learn.microsoft.com/en-us/azure/azure-sql-edge/)
- [pymssql](https://pymssql.readthedocs.io/)

### Source Data
- [Pagila Database](https://github.com/devrimgunduz/pagila) - Original PostgreSQL schema and data

### Related Tools
- [Docker](https://docs.docker.com/)
- [pytest](https://docs.pytest.org/)
- [psycopg2](https://www.psycopg.org/docs/)
