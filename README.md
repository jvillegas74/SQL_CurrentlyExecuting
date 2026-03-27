# SP_DBA_CurrentlyExec

A comprehensive SQL Server DBA monitoring and troubleshooting stored procedure. One command to see what your server is doing — right now.

**Author:** Javier Villegas — [@jvillegas74](https://github.com/jvillegas74)  
**Version:** 7.20 (Generic/Portable Edition)  
**Compatibility:** SQL Server 2012 and later

---

## What It Does

`SP_DBA_CurrentlyExec` is a single stored procedure that replaces a dozen common DBA queries. Run it with no arguments and get a real-time snapshot of every executing session. Add a flag and get blocking chains, backup history, disk health, job status, Query Store recommendations, AlwaysOn lag, and more — all from one command.

---

## Quick Start

```sql
-- Deploy (run once in your target database)
-- See SP_DBA_CurrentlyExec_Generic.sql

-- Run with no arguments: shows all currently executing sessions
EXEC [dbo].[SP_DBA_CurrentlyExec]

-- Get help
EXEC [dbo].[SP_DBA_CurrentlyExec] @help = 1
```

---

## Installation

1. Clone or download this repository
2. Open `SP_DBA_CurrentlyExec_Generic.sql` in SSMS
3. Connect to your target SQL Server instance
4. Select the database where you want to install the procedure (e.g. `master` or a DBA utility database)
5. Execute the script

**Required permissions:**
- `VIEW SERVER STATE` — for DMV access
- `EXECUTE` on `xp_msver`, `xp_regread`, `xp_sqlagent_enum_jobs`, `sp_configure`, `sp_readerrorlog`
- Read access to `msdb` — for backup history and SQL Agent info

---

## Features

### Real-Time Activity
The default mode (no parameters) returns every session currently in `running`, `runnable`, `suspended`, or `rollback` state.

Each row includes:

| Column | Description |
|---|---|
| `SPID` | Session ID. Negated + `*` suffix = this session is also blocking others. `(BSid)` = blocked by this SPID |
| `CPU/SQL` | Total server CPU % / SQL Server CPU % at time of capture |
| `Status` | running / runnable / suspended / rollback |
| `Start_Time` | When the request started |
| `Elap_time` | Elapsed time as `D:HH:MM:SS` |
| `T.CPU` | Cumulative CPU ms used by this session |
| `T.RD / T.WR` | Total logical reads / writes |
| `W.TDB` | TempDB pages allocated |
| `SQL_Text` | Current statement (XML-safe) |
| `DBName / %LogUsed` | Database name with transaction log % used |
| `Object_Name` | Schema-qualified object name (e.g. `dbo.SP_MyProc`) |
| `Wait_Type / Wait_Time` | Current wait |
| `Program_Name` | Application name (SQL Agent jobs decoded to job name) |
| `blocking_text` | SQL text of the blocking session |

---

### All Parameters

#### Filtering (activity result set)

| Parameter | Type | Description |
|---|---|---|
| `@Filter_Name` | varchar(100) | Column to filter on: `SPID`, `Login_Name`, `Host_Name`, `Program_Name`, `object_name` |
| `@Filter_Value` | varchar(100) | Value to match (partial LIKE match supported) |

#### Activity display options

| Parameter | Default | Description |
|---|---|---|
| `@IncludeSystem` | 0 | Include sessions on system wait types (WAITFOR, TRACEWRITE, etc.) |
| `@OrderBy` | NULL | Sort: `SID`, `SPID`, `Status`, `T.CPU`, `CPU`, `Start_Time`, `Elap_Time`, `Object_Name`, `T.RD`, `T.WD`, `W.TDB`, `Wait_Time`, `Host_Name`, `Program_Name`, `Login_Name` |
| `@Blocks` | 0 | `1` = show blocking chain only |
| `@MultiServer` | 0 | `1` = blocking result set only (for aggregation across linked servers) |
| `@excel` | 0 | `1` = clean XML encoding (`&lt;` → `<`) for paste to Excel |
| `@OpenTran` | 0 | `1` = show active open user transactions |

#### Server & configuration

| Parameter | Default | Description |
|---|---|---|
| `@Server_Info` | 0 | Server config summary: SQL version, edition, MAXDOP, memory, CPU, service account, IFI, Lock Pages, trace flags, TCP port, OS version, AlwaysOn |
| `@Info` | 0 | Alias for `@Server_Info` |
| `@sqlservice` | 0 | SQL Server service status, startup type, and service accounts |

#### Database & storage

| Parameter | Default | Description |
|---|---|---|
| `@databases` | 0 | All databases: state, recovery model, backup dates, log %, AlwaysOn sync lag |
| `@backup` | 0 | Full backup and transaction log backup/restore history per database |
| `@disks` | 0 | Disk free space per volume mount point |
| `@diskslatency` | 0 | Drive-level I/O read/write/overall latency in ms |

#### High Availability

| Parameter | Default | Description |
|---|---|---|
| `@AG` | 0 | AlwaysOn AG dashboard: group info, replica roles, database sync state, estimated data loss, redo lag |

#### SQL Agent

| Parameter | Default | Description |
|---|---|---|
| `@jobs` | 0 | All jobs: enabled status, last run outcome, currently running step, execution time, last failure message |

#### Diagnostics

| Parameter | Default | Description |
|---|---|---|
| `@dbmail` | 0 | Database Mail sent/failed/unsent items and event log (last hour) |
| `@errorlog` | 0 | Key entries from SQL Server error log (header line + netbios + dump entries) |
| `@querystore` | 0 | Query Store status per database + auto-tuning recommendations (plan regression detection) |

#### Informational

| Parameter | Default | Description |
|---|---|---|
| `@version` | 0 | Display SP version and build info |
| `@help` | 0 | Display help text and examples |

---

## Usage Examples

```sql
-- All currently executing sessions (default)
EXEC [dbo].[SP_DBA_CurrentlyExec]

-- Filter by object name (partial match)
EXEC [dbo].[SP_DBA_CurrentlyExec] @Filter_Name = 'object_name', @Filter_Value = 'SP_MyProc'

-- Filter by login
EXEC [dbo].[SP_DBA_CurrentlyExec] @Filter_Name = 'Login_Name', @Filter_Value = 'mylogin'

-- Filter by hostname
EXEC [dbo].[SP_DBA_CurrentlyExec] @Filter_Name = 'Host_Name', @Filter_Value = 'APPSERVER01'

-- Filter by SPID
EXEC [dbo].[SP_DBA_CurrentlyExec] @Filter_Name = 'SPID', @Filter_Value = '371'

-- Show only blocking chains
EXEC [dbo].[SP_DBA_CurrentlyExec] @Blocks = 1

-- Active open transactions
EXEC [dbo].[SP_DBA_CurrentlyExec] @OpenTran = 1

-- Server configuration summary
EXEC [dbo].[SP_DBA_CurrentlyExec] @Server_Info = 1

-- Database health (state, backups, AG sync)
EXEC [dbo].[SP_DBA_CurrentlyExec] @databases = 1

-- Backup and restore history
EXEC [dbo].[SP_DBA_CurrentlyExec] @backup = 1

-- Disk free space
EXEC [dbo].[SP_DBA_CurrentlyExec] @disks = 1

-- Disk I/O latency
EXEC [dbo].[SP_DBA_CurrentlyExec] @diskslatency = 1

-- AlwaysOn AG dashboard
EXEC [dbo].[SP_DBA_CurrentlyExec] @AG = 1

-- SQL Agent job status
EXEC [dbo].[SP_DBA_CurrentlyExec] @jobs = 1

-- Database Mail queue (last hour)
EXEC [dbo].[SP_DBA_CurrentlyExec] @dbmail = 1

-- SQL Server error log (key entries)
EXEC [dbo].[SP_DBA_CurrentlyExec] @errorlog = 1

-- SQL Server service status and accounts
EXEC [dbo].[SP_DBA_CurrentlyExec] @sqlservice = 1

-- Query Store status and plan regression recommendations
EXEC [dbo].[SP_DBA_CurrentlyExec] @querystore = 1

-- Excel-friendly output (clean XML encoding, easy copy/paste)
EXEC [dbo].[SP_DBA_CurrentlyExec] @excel = 1

-- Sort by elapsed time descending
EXEC [dbo].[SP_DBA_CurrentlyExec] @OrderBy = 'Elap_Time'

-- Sort by CPU descending, include system wait types
EXEC [dbo].[SP_DBA_CurrentlyExec] @OrderBy = 'T.CPU', @IncludeSystem = 1

-- Version info
EXEC [dbo].[SP_DBA_CurrentlyExec] @version = 1
```

---

## Reading the SPID Column

The `SPID` column is formatted to immediately communicate blocking relationships:

| Format | Meaning |
|---|---|
| `88` | Normal session, not involved in blocking |
| `88(55) *` | Session 88 is blocked by session 55 |
| `-88 *` | Session 88 is blocking other sessions (SID negated as a visual indicator) |
| `88 // PC= 12.500%` | Session 88 is in progress with 12.5% completion (e.g. backup, DBCC) |

---

## Understanding @Server_Info Output

Running `@Server_Info = 1` returns a single-row summary including:

- SQL Server version and edition
- MAXDOP current vs. recommended (based on NUMA topology)
- Cost threshold for parallelism
- Optimize for ad hoc workloads setting
- Max server memory (configured vs. running)
- SQL Server start time
- Service account
- Lock Pages in Memory (Y/N)
- Instant File Initialization (Y/N)
- Active trace flags
- CPU count (flagged if >40 cores without a Core license)
- Physical memory
- Is Clustered
- AlwaysOn Availability Group summary (XML)
- OS version
- IP address
- TCP listening port

---

## Files

| File | Description |
|---|---|
| `SP_DBA_CurrentlyExec_Generic.sql` | Main stored procedure (portable, no proprietary dependencies) |
| `README.md` | This file |

---

## Version History

| Version | Date | Notes |
|---|---|---|
| 4.1 | 2013-08-23 | Initial version |
| 4.2 | 2014-04-23 | Converted to stored procedure |
| 4.3 | 2014-04-25 | Auto-include blocking, open trans, server info |
| 4.4 | 2014-06-10 | Elapsed time includes days |
| 4.5 | 2015-10-25 | Schema-qualified object names; database state filter |
| 4.6 | 2016-04-15 | MultiServer mode; Blocks mode |
| 4.8 | 2016-06-06 | Excel output mode |
| 5.0 | 2017-03-17 | Database and backup info |
| 5.1 | 2017-06-02 | Disk space info |
| 6.2 | 2017-07-28 | Multiple parameters; mail, disks, xevents, jobs, errorlog |
| 6.23 | 2017-11-08 | Trace flag status |
| 6.24 | 2017-11-29 | TCP listening port |
| 6.28 | 2018-01-18 | SQL service status (`@sqlservice`) |
| 6.30 | 2018-09-04 | Improved SQL Agent job info |
| 7.00 | 2021-01-31 | Service account, Lock Pages in Memory, IFI, disk latency, AlwaysOn AG |
| 7.18 | 2023-03-18 | Query Store info; job fixes |
| 7.20 | 2023-11-02 | Bug fixes for @users and @errorlog |
| Generic | 2024 | Removed all proprietary dependencies; portable edition |

---

## Permissions Required

```sql
-- Minimum permissions for the executing login
GRANT VIEW SERVER STATE TO [your_login];

-- If using @jobs or @dbmail
GRANT SELECT ON msdb.dbo.sysjobs          TO [your_login];
GRANT SELECT ON msdb.dbo.sysjobhistory    TO [your_login];
GRANT SELECT ON msdb.dbo.sysmail_sentitems TO [your_login];
-- (or add to SQLAgentReaderRole in msdb)
```

---

## Notes

- The procedure is safe to run on production servers. It uses `NOLOCK` hints on historical/catalog tables and reads only from DMVs and system catalogs.
- The `@excel` flag strips XML entities (`&lt;`, `&gt;`, `&#x0D;`) from SQL text so the output can be pasted directly into Excel without formatting issues.
- SQL Agent job names are automatically decoded in the `Program_Name` column — no need to cross-reference job IDs manually.
- The `@querystore` flag iterates across all databases with Query Store enabled and surfaces plan regression recommendations from `sys.dm_db_tuning_recommendations`.
- `@xevents`, `@dbclog`, and `@users` are present as parameters but return an informational message in the generic edition, as they required proprietary application databases in the original version.

---

## License

MIT License — free to use, modify, and distribute. Attribution appreciated.
