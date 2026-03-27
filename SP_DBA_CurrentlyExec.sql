-- ============================================================================================================================
-- Stored Procedure : SP_DBA_CurrentlyExec
-- Description      : Comprehensive SQL Server DBA monitoring and troubleshooting tool.
--                    Provides real-time activity monitoring, blocking analysis, database
--                    health, backup status, disk usage, job status, Query Store info,
--                    AlwaysOn AG status, SQL error log analysis, and more.
--
-- Author           : Javier Villegas  (jvillegas74 on GitHub)
-- GitHub           : https://github.com/jvillegas74/SQL_CurrentlyExecuting
-- Version          : 7.20 (Generic/Portable edition - dependencies removed)
--
-- Compatibility    : SQL Server 2012 and later (standard DMVs only, no proprietary DB dependencies)
--
-- Change Log
-- v4.1  2013-08-23  Initial version
-- v4.2  2014-04-23  Converted to stored procedure
-- v4.3  2014-04-25  Automatically include blocking reports, open trans and server info
-- v4.31 2014-05-14  Fix Elap_Time = 23:59:59
-- v4.32 2014-05-27  Fix OpenTran
-- v4.4  2014-06-10  Elap_Time includes days; default sort is Elap_Time / Start_Time
-- v4.41 2014-06-22  Minor fixes
-- v4.42 2014-07-28  Fix transaction log usage on named instances
-- v4.43 2014-08-04  Add "Rollback" status
-- v4.5  2015-10-25  Add object schema and databases.state=0 // filters
-- v4.6  2016-04-15  Add MultiServer and Blocks
-- v4.7  2016-04-19  Identify blocking transactions
-- v4.8  2016-06-06  Excel output
-- v4.81 2016-09-23  Add errors related to multiple rows in blocking text
-- v5.0  2017-03-17  Add database and backup info
-- v5.1  2017-06-02  Add disk space info
-- v6.0  2017-06-02  New single input param: Help
-- v6.2  2017-07-28  Return to multiple parameters; adjust @server_info; add mail, disks, xevents, jobs, error log
-- v6.21 2017-08-06  Minor fixes; add Users
-- v6.22 2017-08-11  Fix Info
-- v6.23 2017-11-08  Add trace flag status to @Info output
-- v6.24 2017-11-29  Add listening port
-- v6.25 2017-12-12  Fix databases output to show only one entry per db
-- v6.26 2018-01-08  Fix errorlog
-- v6.27 2018-01-10  Add @dbclog
-- v6.28 2018-01-18  Add @sqlservice
-- v6.30 2018-09-04  Improve SQL Jobs info
-- v7.00 2021-01-31  Improvements: service account, lock pages in memory, IFI, disk latency, AG support
-- v7.10 2022-03-07  Minor fixes
-- v7.15 2023-03-17  Add MT machine name in @users
-- v7.18 2023-03-18  Add Query Store info; fix @jobs
-- v7.20 2023-11-02  Fix @users; fix @errorlog
-- [Generic] 2024    Removed all  proprietary database dependencies.
--                    @dbclog and @xevents flags disabled (they required proprietary databases).
--                   
--                    @errorlog now uses a local temp table instead of proprietary database.
--                    @Server_Info/@Info cleaned of all proprietary database version references.
--
-- ============================================================================================================================
-- USAGE EXAMPLES
-- ============================================================================================================================
-- Basic activity (default):
--   EXEC [dbo].[SP_DBA_CurrentlyExec]
--
-- Filter by specific criteria:
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @Filter_Name='object_name', @Filter_Value='SP_MyProc'
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @Filter_Name='Host_Name',   @Filter_Value='MYSERVER01'
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @Filter_Name='Login_Name',  @Filter_Value='mylogin'
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @Filter_Name='SPID',        @Filter_Value='371'
--
-- Feature flags (set to 1 to enable):
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @Server_Info=1   -- Server configuration summary
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @Info=1          -- Alias for @Server_Info
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @databases=1     -- Database health and AG sync lag
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @backup=1        -- Backup/restore history
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @disks=1         -- Disk free space
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @diskslatency=1  -- Disk I/O latency
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @AG=1            -- AlwaysOn AG dashboard
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @jobs=1          -- SQL Agent job status
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @dbmail=1        -- Database mail queue
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @errorlog=1      -- SQL Server error log (key entries)
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @querystore=1    -- Query Store status and tuning recommendations
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @sqlservice=1    -- SQL Server service status and accounts
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @OpenTran=1      -- Active open transactions
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @Blocks=1        -- Blocking sessions only
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @MultiServer=1   -- Multi-server mode (blocking result set only)
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @excel=1         -- Excel-friendly output (clean XML encoding)
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @IncludeSystem=1 -- Include system wait types
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @version=1       -- Display SP version info
--   EXEC [dbo].[SP_DBA_CurrentlyExec] @help=1          -- Display help text
-- ============================================================================================================================

CREATE OR ALTER PROCEDURE [dbo].[SP_DBA_CurrentlyExec]
(
    -- -------------------------------------------------------
    -- Filtering parameters (for the main activity result set)
    -- -------------------------------------------------------
    @Filter_Name  varchar(100) = NULL,  -- Column to filter on: 'SPID','Login_Name','Host_Name','Program_Name','object_name'
    @Filter_Value varchar(100) = NULL,  -- Value to filter by (supports LIKE partial match)

    -- -------------------------------------------------------
    -- Activity display options
    -- -------------------------------------------------------
    @IncludeSystem bit          = 0,    -- 1 = include system wait types (WAITFOR, TRACEWRITE, etc.)
    @OrderBy       varchar(20)  = NULL, -- Sort column: 'SID','SPID','Status','T.CPU','CPU','Start_Time','Elap_Time',
                                        --              'Object_Name','T.RD','T.WD','W.TDB','Wait_Time',
                                        --              'Host_Name','Program_Name','Login_Name'
    @OpenTran      bit          = 0,    -- 1 = return active open transactions
    @MultiServer   bit          = 0,    -- 1 = run in multi-server mode (returns single blocking result set)
    @Blocks        bit          = 0,    -- 1 = display blocking information only
    @excel         bit          = 0,    -- 1 = format output for copy/paste to Excel (cleans XML encoding)

    -- -------------------------------------------------------
    -- Server / configuration info
    -- -------------------------------------------------------
    @Server_Info   bit          = 0,    -- 1 = show server configuration summary
    @Info          bit          = 0,    -- 1 = alias for @Server_Info

    -- -------------------------------------------------------
    -- Database / storage info
    -- -------------------------------------------------------
    @databases     bit          = 0,    -- 1 = show database health, state, backup dates, AG sync lag
    @backup        bit          = 0,    -- 1 = show backup and restore history per database
    @disks         bit          = 0,    -- 1 = show disk free space per volume
    @diskslatency  bit          = 0,    -- 1 = show disk I/O read/write latency

    -- -------------------------------------------------------
    -- High Availability
    -- -------------------------------------------------------
    @AG            bit          = 0,    -- 1 = show AlwaysOn Availability Group dashboard

    -- -------------------------------------------------------
    -- SQL Agent
    -- -------------------------------------------------------
    @jobs          bit          = 0,    -- 1 = show SQL Agent job status (enabled, last run, running steps, failures)

    -- -------------------------------------------------------
    -- Diagnostics
    -- -------------------------------------------------------
    @dbmail        bit          = 0,    -- 1 = show Database Mail sent/failed/unsent items (last hour)
    @errorlog      bit          = 0,    -- 1 = show key entries from SQL Server error log
    @sqlservice    bit          = 0,    -- 1 = show SQL Server service status and service accounts
    @querystore    bit          = 0,    -- 1 = show Query Store status and auto-tuning recommendations

    -- -------------------------------------------------------
    -- Informational / deprecated flags
    -- -------------------------------------------------------
    @xevents       bit          = 0,    -- NOTE: Disabled in generic edition (required proprietary database)
    @version       bit          = 0,    -- 1 = display stored procedure version
    @help          bit          = 0     -- 1 = display help/usage text
)
AS
-- ============================================================================================================================
-- Initialization
-- ============================================================================================================================
SET NOCOUNT ON
SET ANSI_PADDING ON
SET QUOTED_IDENTIFIER ON

-- @ExitCondition is set to 1 by any "info" block so the main activity section is skipped
DECLARE @ExitCondition bit = 0

-- ============================================================================================================================
-- @version : Display version information
-- ============================================================================================================================
IF @version = 1
BEGIN
    DECLARE @VERSION_TABLE TABLE (ID int, [Value] varchar(100))
    INSERT INTO @VERSION_TABLE (ID, [Value]) VALUES (1, 'Version 7.20 (Generic/Portable Edition)')
    INSERT INTO @VERSION_TABLE (ID, [Value]) VALUES (2, 'By Javier Villegas | github.com/jvillegas74')
    INSERT INTO @VERSION_TABLE (ID, [Value]) VALUES (3, 'Running on ' + @@SERVERNAME)
    INSERT INTO @VERSION_TABLE (ID, [Value]) VALUES (4, 'At ' + CAST(GETDATE() AS varchar(50)))
    SELECT [Value] FROM @VERSION_TABLE ORDER BY ID
    RETURN
END

-- ============================================================================================================================
-- @help : Display usage help
-- ============================================================================================================================
IF @help = 1
BEGIN
    PRINT '
  SP_DBA_CurrentlyExec - Generic Edition

  PARAMETERS:
    @Filter_Name   : SPID | Login_Name | Host_Name | Program_Name | object_name
    @Filter_Value  : value to match (LIKE partial match supported)
    @IncludeSystem : 1 = include system wait type sessions
    @OrderBy       : SID | SPID | Status | T.CPU | CPU | Start_Time | Elap_Time |
                     Object_Name | T.RD | T.WD | W.TDB | Wait_Time |
                     Host_Name | Program_Name | Login_Name
    @OpenTran      : 1 = show active open transactions
    @Server_Info   : 1 = show server configuration summary
    @Info          : 1 = alias for @Server_Info
    @MultiServer   : 1 = multi-server blocking mode
    @Blocks        : 1 = blocking sessions only
    @excel         : 1 = Excel-friendly output
    @databases     : 1 = database health and AG sync info
    @backup        : 1 = backup/restore history
    @disks         : 1 = disk free space
    @diskslatency  : 1 = disk I/O latency
    @AG            : 1 = AlwaysOn AG dashboard
    @jobs          : 1 = SQL Agent job status
    @dbmail        : 1 = Database Mail queue (last hour)
    @errorlog      : 1 = SQL Server error log key entries
    @sqlservice    : 1 = SQL Server service status and accounts
    @querystore    : 1 = Query Store status and tuning recommendations
    @version       : 1 = display version info
    @help          : 1 = display this help text

  NOTE: @xevents, @dbclog, and @users are disabled in the generic edition.

  EXAMPLES:
    EXEC [dbo].[SP_DBA_CurrentlyExec]
    EXEC [dbo].[SP_DBA_CurrentlyExec] @Filter_Name=''object_name'', @Filter_Value=''SP_MyProc''
    EXEC [dbo].[SP_DBA_CurrentlyExec] @Blocks=1
    EXEC [dbo].[SP_DBA_CurrentlyExec] @Server_Info=1
    EXEC [dbo].[SP_DBA_CurrentlyExec] @databases=1
    EXEC [dbo].[SP_DBA_CurrentlyExec] @backup=1
    EXEC [dbo].[SP_DBA_CurrentlyExec] @disks=1
    EXEC [dbo].[SP_DBA_CurrentlyExec] @jobs=1
    EXEC [dbo].[SP_DBA_CurrentlyExec] @errorlog=1
    EXEC [dbo].[SP_DBA_CurrentlyExec] @querystore=1
    EXEC [dbo].[SP_DBA_CurrentlyExec] @AG=1
    EXEC [dbo].[SP_DBA_CurrentlyExec] @sqlservice=1
    EXEC [dbo].[SP_DBA_CurrentlyExec] @version=1
'
    RETURN
END

-- ============================================================================================================================
-- Temp table cleanup (defensive, in case of prior aborted run)
-- ============================================================================================================================
IF OBJECT_ID('tempdb..#TMP_CEXEC') IS NOT NULL DROP TABLE #TMP_CEXEC;

-- ============================================================================================================================
-- @Server_Info / @Info : Server configuration summary
-- ============================================================================================================================
IF @Server_Info = 1 OR @Info = 1
BEGIN
    SET NOCOUNT ON

    DECLARE
        @date           datetime,
        @start          int,
        @ver            varchar(200),
        @config_value   varchar(20),
        @run_value      varchar(20),
        @sqlstart       datetime,
        @hyperthread_ratio  int,
        @optimal_maxdop     int,
        @MirrorServer   varchar(50),
        @IPAddress      varchar(50),
        @maxsrvmemory   bigint,
        @alwaysOn       xml,
        @OSversion      varchar(100),
        @Server_Type    varchar(20),
        @VersionNr      varchar(200)

    SELECT @date = GETDATE()

    -- Determine SQL Server build version string
    SELECT @ver = LEFT(@@VERSION, CHARINDEX(')', @@VERSION, 1))

    -- Append friendly SQL version label
    IF LEFT(@ver, 2) = '10' SET @ver = @ver + ' (SQL 2008)'
    IF LEFT(@ver, 2) = '11' SET @ver = @ver + ' (SQL 2012)'
    IF LEFT(@ver, 2) = '12' SET @ver = @ver + ' (SQL 2014)'
    IF LEFT(@ver, 2) = '13' SET @ver = @ver + ' (SQL 2016)'
    IF LEFT(@ver, 2) = '14' SET @ver = @ver + ' (SQL 2017)'
    IF LEFT(@ver, 2) = '15' SET @ver = @ver + ' (SQL 2019)'
    IF LEFT(@ver, 2) = '16' SET @ver = @ver + ' (SQL 2022)'
    IF LEFT(@ver, 2) = '17' SET @ver = @ver + ' (SQL 2025)'

    -- -------------------------------------------------------
    -- sp_configure: read key settings via temp table
    -- (avoids needing to toggle 'show advanced options')
    -- -------------------------------------------------------
    CREATE TABLE #SPCONFIG
    (
        name         nvarchar(1000),
        minimun      int NOT NULL,
        maximun      int NOT NULL,
        config_value int NOT NULL,
        run_value    int NOT NULL
    )

    BEGIN TRY
        INSERT INTO #SPCONFIG EXEC sp_configure 'max degree of parallelism';
        INSERT INTO #SPCONFIG EXEC sp_configure 'max server memory (MB)';
        INSERT INTO #SPCONFIG EXEC sp_configure 'optimize for ad hoc workloads';
        INSERT INTO #SPCONFIG EXEC sp_configure 'cost threshold for parallelism';
    END TRY
    BEGIN CATCH
        PRINT 'Note: some sp_configure values could not be read. Enable ''Show advanced options'' if needed.';
    END CATCH;

    SELECT @config_value = RTRIM(CONVERT(varchar(8), config_value)),
           @run_value    = RTRIM(CONVERT(varchar(8), run_value))
    FROM #SPCONFIG WHERE name = 'max degree of parallelism'

    SELECT @maxsrvmemory = CAST(RTRIM(CONVERT(varchar(20), run_value)) AS bigint)
    FROM #SPCONFIG WHERE name = 'max server memory (MB)'

    DECLARE @optimize_ad_hoc INT, @cost_thr_parall int
    SELECT @optimize_ad_hoc  = RTRIM(CONVERT(varchar(8), config_value)) FROM #SPCONFIG WHERE name = 'optimize for ad hoc workloads'
    SELECT @cost_thr_parall  = RTRIM(CONVERT(varchar(8), config_value)) FROM #SPCONFIG WHERE name = 'cost threshold for parallelism'

    DROP TABLE #SPCONFIG

    -- -------------------------------------------------------
    -- Trace flags currently enabled globally
    -- -------------------------------------------------------
    DECLARE @tracestatus TABLE (TraceFlag int, Status bit, Global bit, Session bit)
    INSERT INTO @tracestatus EXEC ('DBCC TRACESTATUS (-1) WITH NO_INFOMSGS')

    DECLARE @tf varchar(200) = ''
    SELECT @tf = CASE WHEN @tf = '' THEN CAST(TraceFlag AS varchar(6))
                      ELSE @tf + COALESCE(', ' + CAST(TraceFlag AS varchar(6)), '')
                 END
    FROM @tracestatus

    -- -------------------------------------------------------
    -- MAXDOP optimal recommendation based on NUMA topology
    -- -------------------------------------------------------
    SELECT
        @hyperthread_ratio = hyperthread_ratio,
        @optimal_maxdop    = CASE
                                WHEN cpu_count / hyperthread_ratio > 8 THEN 4
                                ELSE CEILING((cpu_count / hyperthread_ratio) * 0.5)
                             END
    FROM sys.dm_os_sys_info;

    -- SQL Server start time (approximated from tempdb creation date)
    SELECT @sqlstart = create_date FROM sys.databases WHERE name = 'tempdb' AND state = 0

    -- -------------------------------------------------------
    -- CPU and Physical Memory (via xp_msver)
    -- -------------------------------------------------------
    DECLARE @CM TABLE
    (
        [Index]          int,
        Name             nvarchar(1000) NOT NULL,
        Internal_Value   int,
        Character_Value  nvarchar(1000)
    )
    INSERT INTO @CM EXEC xp_msver

    DECLARE @CPUN varchar(20), @Mem int
    SELECT @CPUN = CAST(Internal_Value AS varchar(20)) FROM @CM WHERE Name = 'ProcessorCount'
    SELECT @Mem  = Internal_Value                      FROM @CM WHERE Name = 'PhysicalMemory'

    -- Flag if more than 40 cores but not a Core-licensed Enterprise edition
    IF CAST(@CPUN AS int) > 40 AND CHARINDEX('Core', CAST(SERVERPROPERTY('Edition') AS varchar(100)), 1) = 0
        SET @CPUN = @CPUN + ' (!)'

    -- -------------------------------------------------------
    -- AlwaysOn AG summary (if HADR is enabled)
    -- -------------------------------------------------------
    IF SERVERPROPERTY('IsHadrEnabled') = 1
    BEGIN
        BEGIN TRY
            SELECT @alwaysOn = CAST((
                SELECT
                    AGC.name,
                    RCS.replica_server_name,
                    ARS.role_desc
                FROM sys.availability_groups_cluster AS AGC
                INNER JOIN sys.dm_hadr_availability_replica_cluster_states AS RCS ON RCS.group_id = AGC.group_id
                INNER JOIN sys.dm_hadr_availability_replica_states AS ARS ON ARS.replica_id = RCS.replica_id
                LEFT  JOIN sys.availability_group_listeners AS AGL ON AGL.group_id = ARS.group_id
                ORDER BY role_desc
                FOR XML PATH ('')
            ) AS xml)
        END TRY
        BEGIN CATCH
            SELECT @alwaysOn = NULL
        END CATCH;
    END

    -- -------------------------------------------------------
    -- TCP Listening Port (via registry)
    -- -------------------------------------------------------
    DECLARE @InstName VARCHAR(16) = @@SERVICENAME
    DECLARE @RegLoc   VARCHAR(100)
    DECLARE @TCPPort  int

    IF @InstName = 'MSSQLSERVER'
        SET @RegLoc = 'Software\Microsoft\MSSQLServer\MSSQLServer\SuperSocketNetLib\Tcp\'
    ELSE
        SET @RegLoc = 'Software\Microsoft\Microsoft SQL Server\' + @InstName + '\MSSQLServer\SuperSocketNetLib\Tcp\'

    IF OBJECT_ID('tempdb..#TCPPort') IS NOT NULL DROP TABLE #TCPPort;
    CREATE TABLE #TCPPort ([Value] varchar(100), [Data] int)
    INSERT INTO #TCPPort EXEC [master].[dbo].[xp_regread] 'HKEY_LOCAL_MACHINE', @RegLoc, 'tcpPort'
    SELECT @TCPPort = [Data] FROM #TCPPort

    -- OS version from @@VERSION string (after ' ON ')
    SELECT @OSversion = RIGHT(@@VERSION, LEN(@@VERSION) - 3 - CHARINDEX(' ON ', @@VERSION))

    -- Current connection IP address
    SELECT @IPAddress = dec.local_net_address
    FROM sys.dm_exec_connections AS dec
    WHERE dec.session_id = @@SPID

    -- -------------------------------------------------------
    -- Service account, Lock Pages in Memory, Instant File Initialization
    -- (requires VIEW SERVER STATE permission; available SQL 2012+)
    -- -------------------------------------------------------
    DECLARE @service_account                    VARCHAR(200),
            @instant_file_initialization_enabled varchar(1),
            @lock_pages_in_mem                   varchar(1)

    SELECT
        @service_account                     = service_account,
        @instant_file_initialization_enabled = instant_file_initialization_enabled,
        @lock_pages_in_mem                   = (
            SELECT TOP 1
                CASE WHEN MN.locked_page_allocations_kb = 0 THEN 'N' ELSE 'Y' END
            FROM sys.dm_os_memory_nodes MN
            INNER JOIN sys.dm_os_nodes N ON MN.memory_node_id = N.memory_node_id
            WHERE N.node_state_desc <> 'ONLINE DAC'
            ORDER BY 1 DESC
        )
    FROM sys.dm_server_services
    WHERE servicename LIKE 'SQL Server (%'

    -- Database mirroring partner (if any)
    SELECT TOP 1 @MirrorServer = mirroring_partner_instance
    FROM msdb.sys.database_mirroring (NOLOCK)
    WHERE mirroring_partner_instance IS NOT NULL

    -- -------------------------------------------------------
    -- Final server info result set
    -- -------------------------------------------------------
    SELECT
        @@SERVERNAME                                                          AS [Server Name],
        CAST(GETDATE() AS smalldatetime)                                      AS [Now],
        DATEDIFF(HH, GETUTCDATE(), GETDATE())                                 AS [TZ Offset (hrs)],
        @ver                                                                  AS [SQL Version],
        SERVERPROPERTY('Edition')                                             AS [SQL Edition],
        @Server_Type                                                          AS [Server Type],
        @tf                                                                   AS [Trace Flags],
        @run_value                                                            AS [MAXDOP (run)],
        @optimal_maxdop                                                       AS [MAXDOP (optimal)],
        @hyperthread_ratio                                                    AS [HT Ratio],
        @cost_thr_parall                                                      AS [Cost Threshold Parallelism],
        @optimize_ad_hoc                                                      AS [Optimize for Ad Hoc],
        CONVERT(DATETIME2(0), @sqlstart)                                      AS [SQL Start Time],
        @service_account                                                       AS [Service Account],
        @lock_pages_in_mem                                                    AS [Lock Pages in Mem],
        @instant_file_initialization_enabled                                  AS [IFI],
        CASE SERVERPROPERTY('IsClustered') WHEN 0 THEN 'NO' WHEN 1 THEN 'YES' END AS [Is Cluster],
        @CPUN                                                                 AS [CPU Count],
        @Mem                                                                  AS [Memory (MB)],
        @maxsrvmemory                                                         AS [SQL Max Server Memory (MB)],
        @alwaysOn                                                             AS [Availability Groups],
        @OSversion                                                            AS [OS Version],
        @IPAddress                                                            AS [IP Address],
        @TCPPort                                                              AS [TCP Port]

    SET @ExitCondition = 1
END

-- ============================================================================================================================
-- @diskslatency : Drive-level I/O latency (read, write, overall)
-- ============================================================================================================================
IF @diskslatency = 1
BEGIN
    SELECT
        tab.[Drive],
        tab.volume_mount_point AS [Volume Mount Point],
        CASE WHEN num_of_reads  = 0 THEN 0 ELSE (io_stall_read_ms  / num_of_reads)  END AS [Read Latency (ms)],
        CASE WHEN num_of_writes = 0 THEN 0 ELSE (io_stall_write_ms / num_of_writes) END AS [Write Latency (ms)],
        CASE WHEN (num_of_reads = 0 AND num_of_writes = 0) THEN 0
             ELSE (io_stall / (num_of_reads + num_of_writes))
        END AS [Overall Latency (ms)],
        CASE WHEN num_of_reads  = 0 THEN 0 ELSE (num_of_bytes_read    / num_of_reads)  END AS [Avg Bytes/Read],
        CASE WHEN num_of_writes = 0 THEN 0 ELSE (num_of_bytes_written / num_of_writes) END AS [Avg Bytes/Write],
        CASE WHEN (num_of_reads = 0 AND num_of_writes = 0) THEN 0
             ELSE ((num_of_bytes_read + num_of_bytes_written) / (num_of_reads + num_of_writes))
        END AS [Avg Bytes/Transfer]
    FROM (
        SELECT
            LEFT(UPPER(mf.physical_name), 2)   AS Drive,
            SUM(num_of_reads)                  AS num_of_reads,
            SUM(io_stall_read_ms)              AS io_stall_read_ms,
            SUM(num_of_writes)                 AS num_of_writes,
            SUM(io_stall_write_ms)             AS io_stall_write_ms,
            SUM(num_of_bytes_read)             AS num_of_bytes_read,
            SUM(num_of_bytes_written)          AS num_of_bytes_written,
            SUM(io_stall)                      AS io_stall,
            vs.volume_mount_point
        FROM sys.dm_io_virtual_file_stats(NULL, NULL) AS vfs
        INNER JOIN sys.master_files AS mf WITH (NOLOCK)
            ON vfs.database_id = mf.database_id AND vfs.file_id = mf.file_id
        CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.[file_id]) AS vs
        GROUP BY LEFT(UPPER(mf.physical_name), 2), vs.volume_mount_point
    ) AS tab
    ORDER BY [Overall Latency (ms)] OPTION (RECOMPILE);

    SET @ExitCondition = 1
END

-- ============================================================================================================================
-- @databases : Database health, state, backup dates, AlwaysOn sync lag
-- ============================================================================================================================
IF @databases = 1
BEGIN
    IF OBJECT_ID('tempdb..#TMP_DBs') IS NOT NULL DROP TABLE #TMP_DBs;

    -- Build AlwaysOn commit-time lag info (works even if HADR is not configured)
    ;WITH AG_Stats AS (
        SELECT DISTINCT
            AGS.name                              AS AGGroupName,
            AR.replica_server_name                AS InstanceName,
            HARS.role_desc,
            DB_NAME(DRS.database_id)              AS DBName,
            DRS.database_id,
            AR.availability_mode_desc             AS SyncMode,
            DRS.synchronization_state_desc        AS SyncState,
            DRS.last_hardened_lsn,
            DRS.end_of_log_lsn,
            DRS.last_redone_lsn,
            DRS.last_hardened_time,
            DRS.last_redone_time,
            DRS.log_send_queue_size,
            DRS.redo_queue_size,
            DRS.last_commit_time
        FROM sys.dm_hadr_database_replica_states DRS
        LEFT JOIN sys.availability_replicas AR         ON DRS.replica_id = AR.replica_id
        LEFT JOIN sys.availability_groups AGS          ON AR.group_id = AGS.group_id
        LEFT JOIN sys.dm_hadr_availability_replica_states HARS
            ON AR.group_id = HARS.group_id AND AR.replica_id = HARS.replica_id
    ),
    Pri_CommitTime AS (
        SELECT DBName, last_commit_time FROM AG_Stats WHERE role_desc = 'PRIMARY'
    ),
    Rpt_CommitTime AS (
        SELECT DBName, last_commit_time FROM AG_Stats WHERE role_desc = 'SECONDARY'
    ),
    FO_CommitTime AS (
        SELECT DBName, last_commit_time FROM AG_Stats WHERE role_desc = 'SECONDARY'
    )
    SELECT
        p.[DBName]                                         AS [DatabaseName],
        p.last_commit_time                                 AS [Primary_Last_Commit_Time],
        r.last_commit_time                                 AS [Reporting_Last_Commit_Time],
        DATEDIFF(ss, r.last_commit_time, p.last_commit_time) AS [Reporting_Sync_Lag_(secs)],
        f.last_commit_time                                 AS [FailOver_Last_Commit_Time],
        DATEDIFF(ss, f.last_commit_time, p.last_commit_time) AS [FailOver_Sync_Lag_(secs)]
    INTO #TMP_DBs
    FROM Pri_CommitTime p
    LEFT JOIN Rpt_CommitTime r ON r.[DBName] = p.[DBName]
    LEFT JOIN FO_CommitTime  f ON f.[DBName] = p.[DBName]

    -- Main database listing
    SELECT
        d.name,
        d.recovery_model_desc,
        d.compatibility_level,
        d.user_access_desc,
        d.is_read_only,
        d.state_desc,
        d.is_broker_enabled,
        d.is_trustworthy_on,
        d.log_reuse_wait_desc,
        LOG.cntr_value                           AS [Percent Log Used],
        HA.[Primary_Last_Commit_Time],
        HA.[Reporting_Last_Commit_Time],
        HA.[Reporting_Sync_Lag_(secs)],
        HA.[FailOver_Sync_Lag_(secs)],
        B.last_backup_start_date,
        B.last_backup_finish_date,
        DATEDIFF(mi, last_backup_start_date, last_backup_finish_date) AS [Backup Duration (min)],
        TR.last_TRLog_backup_finish_date,
        R.last_restore_date,
        PA.physical_device_name                  AS BAK_File,
        PAL.physical_device_name                 AS TLog_Backup_File
    FROM sys.databases d
    -- Log used percentage
    LEFT JOIN (
        SELECT instance_name, cntr_value
        FROM sys.dm_os_performance_counters
        WHERE object_name  LIKE '%:Databases%'
          AND counter_name = 'Percent Log Used'
    ) LOG ON d.name = LOG.instance_name
    -- AlwaysOn lag (aggregated)
    LEFT JOIN (
        SELECT
            DatabaseName,
            MIN(Primary_Last_Commit_Time)           AS Primary_Last_Commit_Time,
            MIN(Reporting_Last_Commit_Time)         AS Reporting_Last_Commit_Time,
            MAX([Reporting_Sync_Lag_(secs)])        AS [Reporting_Sync_Lag_(secs)],
            MAX([FailOver_Sync_Lag_(secs)])         AS [FailOver_Sync_Lag_(secs)]
        FROM #TMP_DBs
        GROUP BY DatabaseName
    ) HA ON d.name = HA.DatabaseName
    -- Last full backup
    LEFT JOIN (
        SELECT
            BS.database_name,
            MAX(BS.backup_start_date)  AS last_backup_start_date,
            MAX(BS.backup_finish_date) AS last_backup_finish_date
        FROM msdb.dbo.backupset BS (NOLOCK)
        WHERE BS.backup_start_date >= CAST(CONVERT(varchar(10), DATEADD(mm, -3, GETDATE()), 120) AS datetime)
          AND BS.type = 'D'
        GROUP BY BS.database_name
    ) B ON d.name = B.database_name
    -- Last transaction log backup
    LEFT JOIN (
        SELECT
            BS.database_name,
            MAX(BS.backup_finish_date) AS last_TRLog_backup_finish_date
        FROM msdb.dbo.backupset BS (NOLOCK)
        INNER JOIN msdb.dbo.backupmediafamily MF (NOLOCK) ON BS.media_set_id = MF.media_set_id
        WHERE BS.backup_start_date >= CAST(CONVERT(varchar(10), DATEADD(mm, -1, GETDATE()), 120) AS datetime)
          AND BS.type = 'L'
        GROUP BY BS.database_name
    ) TR ON d.name = TR.database_name
    -- Last restore
    LEFT JOIN (
        SELECT
            rh.destination_database_name,
            MAX(rh.restore_date) AS last_restore_date
        FROM msdb.dbo.restorehistory rh (NOLOCK)
        INNER JOIN msdb.dbo.backupset BS (NOLOCK) ON rh.backup_set_id = BS.backup_set_id
        WHERE BS.type = 'D'
          AND rh.restore_date >= CAST(CONVERT(varchar(10), DATEADD(mm, -3, GETDATE()), 120) AS datetime)
        GROUP BY rh.destination_database_name
    ) R ON d.name = R.destination_database_name
    LEFT JOIN msdb.sys.database_mirroring dm (NOLOCK) ON d.database_id = dm.database_id
    -- Full backup file path
    LEFT JOIN (
        SELECT
            BS.database_name,
            MF.physical_device_name,
            BS.backup_finish_date
        FROM msdb.dbo.backupset BS (NOLOCK)
        INNER JOIN (
            SELECT
                media_set_id,
                MAX(physical_device_name) + ' '
                    + CASE WHEN MAX(family_sequence_number) > 1
                           THEN '(' + CAST(MAX(family_sequence_number) AS varchar(5)) + ')'
                           ELSE '' END AS physical_device_name
            FROM msdb.dbo.backupmediafamily
            GROUP BY media_set_id
        ) MF ON BS.media_set_id = MF.media_set_id
        WHERE [type] = 'D'
    ) PA ON d.name = PA.database_name AND PA.backup_finish_date = B.last_backup_finish_date
    -- TLog backup file path
    LEFT JOIN (
        SELECT
            BS.database_name,
            MF.physical_device_name,
            BS.backup_finish_date
        FROM msdb.dbo.backupset BS (NOLOCK)
        INNER JOIN msdb.dbo.backupmediafamily MF (NOLOCK) ON BS.media_set_id = MF.media_set_id
        WHERE [type] = 'L'
    ) PAL ON d.name = PAL.database_name AND PAL.backup_finish_date = TR.last_TRLog_backup_finish_date
    ORDER BY recovery_model_desc, name

    SET @ExitCondition = 1
END

-- ============================================================================================================================
-- @backup : Backup and restore history per database
-- ============================================================================================================================
IF @backup = 1
BEGIN
    SELECT DISTINCT
        d.name,
        d.database_id                                                             AS dbid,
        B.last_backup_start_date,
        B.last_backup_finish_date,
        DATEDIFF(mi, last_backup_start_date, last_backup_finish_date)            AS [Duration (min)],
        TR.last_TRLog_backup_finish_date,
        R.last_restore_date,
        d.recovery_model_desc,
        d.compatibility_level,
        LOG.cntr_value                                                            AS [% Log Used],
        d.log_reuse_wait_desc,
        PA.physical_device_name                                                  AS BAK_File,
        PAL.physical_device_name                                                 AS TLog_Backup_File
    FROM sys.databases d
    LEFT JOIN (
        SELECT
            BS.database_name,
            MAX(BS.backup_start_date)  AS last_backup_start_date,
            MAX(BS.backup_finish_date) AS last_backup_finish_date
        FROM msdb.dbo.backupset BS (NOLOCK)
        WHERE BS.backup_start_date >= CAST(CONVERT(varchar(10), DATEADD(mm, -3, GETDATE()), 120) AS datetime)
          AND BS.type = 'D'
        GROUP BY BS.database_name
    ) B ON d.name = B.database_name
    LEFT JOIN (
        SELECT
            BS.database_name,
            MAX(BS.backup_finish_date) AS last_TRLog_backup_finish_date
        FROM msdb.dbo.backupset BS (NOLOCK)
        INNER JOIN msdb.dbo.backupmediafamily MF (NOLOCK) ON BS.media_set_id = MF.media_set_id
        WHERE BS.backup_start_date >= CAST(CONVERT(varchar(10), DATEADD(mm, -1, GETDATE()), 120) AS datetime)
          AND BS.type = 'L'
        GROUP BY BS.database_name
    ) TR ON d.name = TR.database_name
    LEFT JOIN (
        SELECT
            rh.destination_database_name,
            MAX(rh.restore_date) AS last_restore_date
        FROM msdb.dbo.restorehistory rh (NOLOCK)
        INNER JOIN msdb.dbo.backupset BS (NOLOCK) ON rh.backup_set_id = BS.backup_set_id
        WHERE BS.type = 'D'
          AND rh.restore_date >= CAST(CONVERT(varchar(10), DATEADD(mm, -3, GETDATE()), 120) AS datetime)
        GROUP BY rh.destination_database_name
    ) R ON d.name = R.destination_database_name
    LEFT JOIN msdb.sys.database_mirroring dm (NOLOCK) ON d.database_id = dm.database_id
    LEFT JOIN (
        SELECT object_name, instance_name, cntr_value
        FROM sys.dm_os_performance_counters
        WHERE object_name  LIKE '%:Databases%'
          AND counter_name = 'Percent Log Used'
    ) LOG ON d.name = LOG.instance_name
    LEFT JOIN (
        SELECT
            media_set_id,
            MAX(database_name)       AS database_name,
            MAX(physical_device_name) AS physical_device_name,
            MAX(backup_finish_date)  AS backup_finish_date
        FROM (
            SELECT
                BS.media_set_id,
                BS.database_name,
                MF.physical_device_name,
                BS.backup_finish_date
            FROM msdb.dbo.backupset BS (NOLOCK)
            INNER JOIN msdb.dbo.backupmediafamily MF (NOLOCK) ON BS.media_set_id = MF.media_set_id
            WHERE [type] = 'D'
        ) X
        GROUP BY media_set_id
    ) PA ON d.name = PA.database_name AND PA.backup_finish_date = B.last_backup_finish_date
    LEFT JOIN (
        SELECT
            BS.database_name,
            MF.physical_device_name,
            BS.backup_finish_date
        FROM msdb.dbo.backupset BS (NOLOCK)
        INNER JOIN msdb.dbo.backupmediafamily MF (NOLOCK) ON BS.media_set_id = MF.media_set_id
        WHERE [type] = 'L'
    ) PAL ON d.name = PAL.database_name AND PAL.backup_finish_date = TR.last_TRLog_backup_finish_date
    ORDER BY last_backup_start_date

    SET @ExitCondition = 1
END

-- ============================================================================================================================
-- @disks : Disk free space per volume
-- ============================================================================================================================
IF @disks = 1
BEGIN
    SELECT DISTINCT
        vs.volume_mount_point                                                     AS [Drive],
        vs.logical_volume_name                                                    AS [Drive Name],
        vs.total_bytes     / 1024 / 1024 / 1024                                  AS [Drive Size GB],
        vs.available_bytes / 1024 / 1024 / 1024                                  AS [Drive Free Space GB],
        (vs.available_bytes / 1024 / 1024) * 100 / (vs.total_bytes / 1024 / 1024) AS [% Free Space]
    FROM sys.master_files AS f
    CROSS APPLY sys.dm_os_volume_stats(f.database_id, f.file_id) AS vs
    ORDER BY vs.volume_mount_point;

    RETURN  -- disk space is a standalone query; exit immediately
END

-- ============================================================================================================================
-- @xevents : Disabled in generic edition (required database and custom functions)
-- ============================================================================================================================
IF @xevents = 1
BEGIN
    SELECT 'NOTE: @xevents is disabled in the generic edition. It required the database and proprietary functions.' AS [Message]
    RETURN
END

-- ============================================================================================================================
-- @dbmail : Database Mail queue (last hour)
-- ============================================================================================================================
IF @dbmail = 1
BEGIN
    SELECT * FROM msdb.dbo.sysmail_sentitems   (NOLOCK) WHERE sent_date           >= DATEADD(hh, -1, GETDATE()) ORDER BY mailitem_id DESC
    SELECT * FROM msdb.dbo.sysmail_faileditems (NOLOCK) WHERE sent_date           >= DATEADD(hh, -1, GETDATE()) ORDER BY mailitem_id DESC
    SELECT * FROM msdb.dbo.sysmail_unsentitems (NOLOCK) WHERE send_request_date   >= DATEADD(hh, -1, GETDATE()) ORDER BY mailitem_id DESC
    SELECT * FROM msdb.dbo.sysmail_event_log   (NOLOCK) WHERE log_date            >= DATEADD(hh, -1, GETDATE())

    SET @ExitCondition = 1
END

-- ============================================================================================================================
-- @errorlog : Key entries from SQL Server error log
-- ============================================================================================================================
IF @errorlog = 1
BEGIN
    -- Use a local temp table (no dependency on external databases)
    IF OBJECT_ID('tempdb..#TMP_SQL_ERRORLOG') IS NOT NULL DROP TABLE #TMP_SQL_ERRORLOG;

    CREATE TABLE #TMP_SQL_ERRORLOG
    (
        ID          int          IDENTITY NOT NULL,
        LogDate     datetime     NOT NULL,
        ProcessInfo varchar(255) NOT NULL,
        [Text]      varchar(max) NOT NULL
    )

    INSERT INTO #TMP_SQL_ERRORLOG
        EXEC master.dbo.sp_readerrorlog

    -- Return the header line and any lines mentioning netbios (instance name) or dump (crash/minidump)
    SELECT *
    FROM #TMP_SQL_ERRORLOG T (NOLOCK)
    WHERE ID = 1
       OR T.[Text] LIKE '%netbios%'
       OR T.[Text] LIKE '%dump%'
    ORDER BY ID

    DROP TABLE #TMP_SQL_ERRORLOG

    SET @ExitCondition = 1
END

-- ============================================================================================================================
-- @jobs : SQL Agent job status (enabled, last run outcome, currently running step, last failure)
-- ============================================================================================================================
IF @jobs = 1
BEGIN
    -- Snapshot of currently running jobs from xp_sqlagent_enum_jobs
    DECLARE @CurrentJobs TABLE
    (
        [Job ID]              uniqueidentifier,
        [Last Run Date]       varchar(255),
        [Last Run Time]       varchar(255),
        [Next Run Date]       varchar(255),
        [Next Run Time]       varchar(255),
        [Next Run Schedule ID] varchar(255),
        [Requested To Run]    varchar(255),
        [Request Source]      varchar(255),
        [Request Source ID]   varchar(255),
        [Running]             varchar(255),
        [Current Step]        varchar(255),
        [Current Retry Attempt] varchar(255),
        [State]               varchar(255)
    )
    INSERT INTO @CurrentJobs
    EXECUTE master.dbo.xp_sqlagent_enum_jobs 1, ''

    SELECT
        t.name                                                 AS job_name,
        [Job Enabled]                                          AS [Enabled],
        [Job LastExec]                                         AS [LastExec],
        CAST(last_run_datetime AS DATETIME2(0))                AS last_run_datetime,
        start_execution_date,
        [execution_time],
        XXX.Step,
        XXX.[Current Retry Attempt],
        XXX.[Request Source],
        XXX.[Request Source ID],
        XXX.[Requested To Run],
        XXX.subsystem,
        CASE WHEN XXX.subsystem = 'ANALYSISQUERY'
             THEN ISNULL(XXX.server, '') + ' / ' + ISNULL(XXX.database_name, '')
             ELSE NULL
        END                                                    AS [Server/Database],
        FAIL.step                                              AS [Last Fail Step],
        FAIL.run_duration                                      AS [Last Fail Duration],
        FAIL.message                                           AS [Failure Message]
    FROM
    (
        -- Base job info with last run outcome
        SELECT
            j.job_id,
            j.name,
            [Job Enabled] = CASE j.Enabled WHEN 1 THEN 'Yes' WHEN 0 THEN 'No' END,
            [Job LastExec] = CASE S.last_run_outcome WHEN 1 THEN 'Success' WHEN 0 THEN 'Failed' ELSE NULL END,
            S.last_run_datetime,
            X.ct,
            X.min_date,
            R.*
        FROM msdb.dbo.sysjobs j (NOLOCK)
        -- Last run outcome and datetime
        INNER JOIN (
            SELECT
                sj.name,
                sjs.last_run_outcome,
                CASE WHEN sjs.last_run_date > 0
                     THEN DATETIMEFROMPARTS(
                            sjs.last_run_date / 10000,
                            sjs.last_run_date / 100 % 100,
                            sjs.last_run_date % 100,
                            sjs.last_run_time / 10000,
                            sjs.last_run_time / 100 % 100,
                            sjs.last_run_time % 100,
                            0)
                     ELSE NULL
                END AS last_run_datetime
            FROM msdb.dbo.sysjobservers sjs
            LEFT JOIN msdb.dbo.sysjobs sj ON sj.job_id = sjs.job_id
        ) S ON j.name = S.name
        -- Run count and earliest recorded date
        LEFT JOIN (
            SELECT
                j.name,
                COUNT(run_duration) AS ct,
                MIN(run_date)       AS min_date
            FROM msdb.dbo.sysjobhistory AS h
            INNER JOIN msdb.dbo.sysjobs AS j ON h.job_id = j.job_id
            WHERE h.step_id = 0
            GROUP BY j.name
        ) X ON j.name = X.name
        -- Currently executing step info
        LEFT JOIN (
            SELECT
                j.name                                                        AS job_name,
                CAST(ja.start_execution_date AS DATETIME2(0))                 AS start_execution_date,
                CONVERT(time(0), DATEADD(SECOND, DATEDIFF(ss, ja.start_execution_date, GETDATE()), 0)) AS execution_time,
                ISNULL(last_executed_step_id, 0) + 1                         AS current_executed_step_id,
                js.step_name
            FROM msdb.dbo.sysjobactivity ja
            LEFT JOIN msdb.dbo.sysjobhistory jh ON ja.job_history_id = jh.instance_id
            JOIN msdb.dbo.sysjobs j             ON ja.job_id = j.job_id
            JOIN msdb.dbo.sysjobsteps js        ON ja.job_id = js.job_id
                AND ISNULL(ja.last_executed_step_id, 0) + 1 = js.step_id
            WHERE ja.session_id = (SELECT TOP 1 session_id FROM msdb.dbo.syssessions ORDER BY agent_start_date DESC)
              AND start_execution_date IS NOT NULL
              AND stop_execution_date  IS NULL
              AND j.name NOT LIKE 'cdc.%'
              AND js.subsystem <> 'Distribution'
        ) R ON j.name = R.job_name
    ) AS t
    -- Currently running step details (from xp_sqlagent_enum_jobs)
    LEFT JOIN (
        SELECT
            cj.[Job ID],
            sj.name,
            sj.enabled,
            cj.[Request Source],
            cj.[Request Source ID],
            cj.[Requested To Run],
            CAST(cj.[Current Step] AS varchar(10)) + ' - ' + st.step_name AS [Step],
            cj.[Current Retry Attempt],
            st.subsystem,
            st.server,
            st.database_name
        FROM @CurrentJobs cj
        JOIN msdb.dbo.sysjobs     sj ON cj.[Job ID]      = sj.job_id
        JOIN msdb.dbo.sysjobsteps st ON cj.[Job ID]      = st.job_id
                                     AND cj.[Current Step] = st.step_id
        WHERE Running = 1
    ) XXX ON XXX.[Job ID] = t.[Job_ID]
    -- Last failed step info
    LEFT JOIN (
        SELECT
            job_id,
            CAST(step_id AS varchar(5)) + ' - ' + step_name AS [Step],
            run_duration,
            message
        FROM (
            SELECT
                j.job_id,
                j.name,
                s.step_name,
                s.step_id,
                CASE hd.run_status
                    WHEN 0 THEN 'Failed'
                    WHEN 1 THEN 'Succeeded'
                    WHEN 2 THEN 'Retry'
                    WHEN 3 THEN 'Cancelled'
                    WHEN 4 THEN 'In Progress'
                END AS ExecutionStatus,
                CAST(
                    CONVERT(CHAR(10), CAST(STR(hd.run_date, 8, 0) AS datetime), 111)
                    + ' '
                    + STUFF(STUFF(RIGHT('000000' + CAST(hd.run_time AS varchar(6)), 6), 5, 0, ':'), 3, 0, ':')
                AS datetime) AS RunDateTime,
                -- Format run_duration as HH:MM:SS
                ISNULL(CASE LEN(run_duration)
                    WHEN 1 THEN CAST('00:00:0' + CAST(run_duration AS CHAR)     AS CHAR(8))
                    WHEN 2 THEN CAST('00:00:'  + CAST(run_duration AS CHAR)     AS CHAR(8))
                    WHEN 3 THEN CAST('00:0'    + LEFT(RIGHT(run_duration, 3), 1) + ':' + RIGHT(run_duration, 2) AS CHAR(8))
                    WHEN 4 THEN CAST('00:'     + LEFT(RIGHT(run_duration, 4), 2) + ':' + RIGHT(run_duration, 2) AS CHAR(8))
                    WHEN 5 THEN CAST('0'       + LEFT(RIGHT(run_duration, 5), 1) + ':' + LEFT(RIGHT(run_duration, 4), 2) + ':' + RIGHT(run_duration, 2) AS CHAR(8))
                    WHEN 6 THEN CAST(            LEFT(RIGHT(run_duration, 6), 2) + ':' + LEFT(RIGHT(run_duration, 4), 2) + ':' + RIGHT(run_duration, 2) AS CHAR(8))
                END, 'NA') AS run_duration,
                s.subsystem,
                hd.message
            FROM msdb.dbo.sysjobs j (NOLOCK)
            INNER JOIN msdb.dbo.sysjobsteps s (NOLOCK) ON s.job_id = j.job_id
            INNER JOIN (
                SELECT job_id, step_id, MAX(instance_id) AS instance_id
                FROM msdb.dbo.sysjobhistory (NOLOCK)
                GROUP BY job_id, step_id
            ) H ON H.job_id = j.job_id AND s.step_id = H.step_id
            INNER JOIN msdb.dbo.sysjobhistory hd (NOLOCK) ON hd.instance_id = H.instance_id
            WHERE hd.run_status IN (0, 2, 3)  -- Failed, Retry, Cancelled
        ) CC
    ) FAIL ON FAIL.job_id = t.job_id
    ORDER BY job_name;

    SET @ExitCondition = 1
END

-- Early exit if any "info" mode flag was processed
IF @ExitCondition = 1 RETURN

-- ============================================================================================================================
-- @sqlservice : SQL Server service status and service accounts
-- ============================================================================================================================
IF @sqlservice = 1
BEGIN
    -- Returns status, startup type, service account, and cluster info for SQL services
    SELECT
        servicename,
        startup_type,
        startup_type_desc,
        status,
        status_desc,
        process_id,
        last_startup_time,
        service_account,
        filename,
        is_clustered,
        cluster_nodename
    FROM sys.dm_server_services

    SET @ExitCondition = 1
END

-- ============================================================================================================================
-- @OpenTran : Active open user transactions
-- ============================================================================================================================
IF @OpenTran = 1
BEGIN
    SELECT
        trans.session_id                                             AS [Session ID],
        trans.transaction_id                                         AS [Transaction ID],
        tas.name                                                     AS [Transaction Name],
        db.name                                                      AS [Database],
        tds.database_transaction_begin_time,
        DATEDIFF(ss, tds.database_transaction_begin_time, GETDATE()) AS [Transaction Time (ss)],
        tx.text                                                      AS [SQL Text]
    FROM sys.dm_tran_active_transactions tas
    INNER JOIN sys.dm_tran_database_transactions tds ON tas.transaction_id  = tds.transaction_id
    INNER JOIN sys.dm_tran_session_transactions trans ON trans.transaction_id = tas.transaction_id
    INNER JOIN sys.databases db                        ON tds.database_id     = db.database_id
    INNER JOIN sys.dm_exec_requests r                  ON trans.session_id    = r.session_id
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) tx
    WHERE trans.is_user_transaction = 1  -- user transactions only
      AND tas.transaction_state = 2      -- active only
    ORDER BY 6 DESC

    SET @ExitCondition = 1
END

-- ============================================================================================================================
-- @AG : AlwaysOn Availability Group dashboard
-- ============================================================================================================================
IF @AG = 1
BEGIN
    -- Snapshot AG DMV data into temp tables for safe cross-join operations
    DROP TABLE IF EXISTS #tmpag_availability_groups
    DROP TABLE IF EXISTS #tmpag_availability_replicas
    DROP TABLE IF EXISTS #tmpag_availability_replica_states
    DROP TABLE IF EXISTS #tmpag_availability_group_states
    DROP TABLE IF EXISTS #tmpdbr_database_replica_states
    DROP TABLE IF EXISTS #tmpdbr_availability_replica_states
    DROP TABLE IF EXISTS #tmpdbr_database_replica_states_primary_LCT
    DROP TABLE IF EXISTS #tmpdbr_database_replica_cluster_states
    DROP TABLE IF EXISTS #tmpdbr_availability_replicas

    SELECT * INTO #tmpdbr_database_replica_states            FROM master.sys.dm_hadr_database_replica_states
    SELECT group_id, primary_replica INTO #tmpag_availability_group_states FROM master.sys.dm_hadr_availability_group_states
    SELECT * INTO #tmpag_availability_groups                 FROM master.sys.availability_groups
    SELECT group_id, replica_id, replica_metadata_id INTO #tmpag_availability_replicas FROM master.sys.availability_replicas
    SELECT replica_id, is_local, role INTO #tmpag_availability_replica_states FROM master.sys.dm_hadr_availability_replica_states
    SELECT replica_id, role, is_local INTO #tmpdbr_availability_replica_states FROM master.sys.dm_hadr_availability_replica_states
    SELECT group_id, replica_id, replica_server_name, availability_mode INTO #tmpdbr_availability_replicas
        FROM master.sys.availability_replicas WHERE availability_mode <> 4
    SELECT replica_id, group_database_id, database_name, is_database_joined, is_failover_ready
        INTO #tmpdbr_database_replica_cluster_states
        FROM master.sys.dm_hadr_database_replica_cluster_states

    -- AG group-level summary
    SELECT
        AG.name                                                       AS [Name],
        ISNULL(AG.automated_backup_preference, 4)                     AS [AutomatedBackupPreference],
        ISNULL(AG.failure_condition_level, 6)                         AS [FailureConditionLevel],
        ISNULL(AG.health_check_timeout, -1)                           AS [HealthCheckTimeout],
        AR2.replica_metadata_id                                       AS [ID],
        ISNULL(arstates2.role, 3)                                     AS [LocalReplicaRole],
        ISNULL(agstates.primary_replica, '')                          AS [PrimaryReplicaServerName],
        AG.group_id                                                   AS [UniqueId],
        CAST(ISNULL(AG.basic_features, 0) AS bit)                     AS [BasicAvailabilityGroup],
        CAST(ISNULL(AG.db_failover, 0) AS bit)                        AS [DatabaseHealthTrigger],
        CAST(ISNULL(AG.dtc_support, 0) AS bit)                        AS [DtcSupportEnabled],
        CAST(ISNULL(AG.is_distributed, 0) AS bit)                     AS [IsDistributedAvailabilityGroup],
        ISNULL(AG.cluster_type, 0)                                    AS [ClusterType],
        ISNULL(AG.required_synchronized_secondaries_to_commit, 0)     AS [RequiredSynchronizedSecondariesToCommit]
    FROM #tmpag_availability_groups AS AG
    LEFT  JOIN #tmpag_availability_group_states  AS agstates   ON AG.group_id     = agstates.group_id
    INNER JOIN #tmpag_availability_replicas       AS AR2        ON AG.group_id     = AR2.group_id
    INNER JOIN #tmpag_availability_replica_states AS arstates2  ON AR2.replica_id  = arstates2.replica_id
                                                                AND arstates2.is_local = 1

    -- Capture primary last commit time for lag calculation
    SELECT ars.role, drs.database_id, drs.replica_id, drs.last_commit_time
    INTO #tmpdbr_database_replica_states_primary_LCT
    FROM #tmpdbr_database_replica_states AS drs
    LEFT JOIN #tmpdbr_availability_replica_states ars ON drs.replica_id = ars.replica_id
    WHERE ars.role = 1

    -- Database replica detail
    SELECT
        AR.replica_server_name                                                                   AS [AvailabilityReplicaServerName],
        dbcs.database_name                                                                       AS [AvailabilityDatabaseName],
        AG.name                                                                                  AS [AvailabilityGroupName],
        ISNULL(dbr.database_id, 0)                                                               AS [DatabaseId],
        ISNULL(dbr.end_of_log_lsn, 0)                                                           AS [EndOfLogLSN],
        CASE dbcs.is_failover_ready WHEN 1 THEN 0
             ELSE ISNULL(DATEDIFF(ss, dbr.last_commit_time, dbrp.last_commit_time), 0) END      AS [EstimatedDataLoss (secs)],
        ISNULL(CASE dbr.redo_rate WHEN 0 THEN -1
               ELSE CAST(dbr.redo_queue_size AS float) / dbr.redo_rate END, -1)                  AS [EstimatedRecoveryTime],
        ISNULL(dbr.filestream_send_rate, -1)                                                     AS [FileStreamSendRate],
        ISNULL(dbcs.is_failover_ready, 0)                                                        AS [IsFailoverReady],
        ISNULL(dbcs.is_database_joined, 0)                                                       AS [IsJoined],
        arstates.is_local                                                                        AS [IsLocal],
        ISNULL(dbr.is_suspended, 0)                                                              AS [IsSuspended],
        ISNULL(dbr.last_commit_lsn, 0)                                                          AS [LastCommitLSN],
        ISNULL(dbr.last_commit_time, 0)                                                          AS [LastCommitTime],
        ISNULL(dbr.last_hardened_lsn, 0)                                                         AS [LastHardenedLSN],
        ISNULL(dbr.last_hardened_time, 0)                                                        AS [LastHardenedTime],
        ISNULL(dbr.last_received_lsn, 0)                                                         AS [LastReceivedLSN],
        ISNULL(dbr.last_received_time, 0)                                                        AS [LastReceivedTime],
        ISNULL(dbr.last_redone_lsn, 0)                                                           AS [LastRedoneLSN],
        ISNULL(dbr.last_redone_time, 0)                                                          AS [LastRedoneTime],
        ISNULL(dbr.last_sent_lsn, 0)                                                             AS [LastSentLSN],
        ISNULL(dbr.last_sent_time, 0)                                                            AS [LastSentTime],
        ISNULL(dbr.log_send_queue_size, -1)                                                      AS [LogSendQueueSize],
        ISNULL(dbr.log_send_rate, -1)                                                            AS [LogSendRate],
        ISNULL(dbr.recovery_lsn, 0)                                                              AS [RecoveryLSN],
        ISNULL(dbr.redo_queue_size, -1)                                                          AS [RedoQueueSize],
        ISNULL(dbr.redo_rate, -1)                                                                AS [RedoRate],
        ISNULL(AR.availability_mode, 2)                                                          AS [ReplicaAvailabilityMode],
        ISNULL(arstates.role, 3)                                                                 AS [ReplicaRole],
        ISNULL(dbr.suspend_reason, 7)                                                            AS [SuspendReason],
        ISNULL(CASE dbr.log_send_rate WHEN 0 THEN -1
               ELSE CAST(dbr.log_send_queue_size AS float) / dbr.log_send_rate END, -1)          AS [SynchronizationPerformance],
        ISNULL(dbr.synchronization_state, 0)                                                     AS [SynchronizationState],
        ISNULL(dbr.truncation_lsn, 0)                                                            AS [TruncationLSN]
    FROM #tmpag_availability_groups AS AG
    INNER JOIN #tmpdbr_availability_replicas            AS AR      ON AR.group_id     = AG.group_id
    INNER JOIN #tmpdbr_database_replica_cluster_states  AS dbcs    ON dbcs.replica_id = AR.replica_id
    LEFT  JOIN #tmpdbr_database_replica_states          AS dbr     ON dbcs.replica_id = dbr.replica_id
                                                                   AND dbcs.group_database_id = dbr.group_database_id
    LEFT  JOIN #tmpdbr_database_replica_states_primary_LCT AS dbrp ON dbr.database_id = dbrp.database_id
    INNER JOIN #tmpdbr_availability_replica_states      AS arstates ON arstates.replica_id = AR.replica_id
    ORDER BY [AvailabilityReplicaServerName] ASC, [AvailabilityDatabaseName] ASC

    -- Quick lag summary (top 2 most behind databases)
    SELECT TOP 2
        db.database_name,
        st.synchronization_state_desc,
        st.synchronization_health_desc,
        st.last_redone_time,
        DATEDIFF(mi, last_redone_time, GETDATE())                                                 AS [Delay (minutes)],
        CONVERT(varchar(6), DATEDIFF(mi, last_redone_time, GETDATE()) / 60)
            + ':' + RIGHT('0' + CONVERT(varchar(2), DATEDIFF(mi, last_redone_time, GETDATE()) % 60), 2) AS [Delay HH:MM],
        xd.cntr_value                                                                             AS [% Log Used]
    FROM #tmpdbr_database_replica_states st
    INNER JOIN #tmpdbr_database_replica_cluster_states db
        ON db.replica_id = st.replica_id AND db.group_database_id = st.group_database_id
    LEFT JOIN (
        SELECT instance_name, cntr_value
        FROM sys.dm_os_performance_counters
        WHERE object_name  LIKE '%Databases%'
          AND counter_name = 'Percent Log Used'
    ) xd ON xd.instance_name = db.database_name
    WHERE last_redone_time IS NOT NULL
    ORDER BY last_redone_time ASC

    SET @ExitCondition = 1
END

-- ============================================================================================================================
-- @querystore : Query Store status and auto-tuning recommendations per database
-- ============================================================================================================================
IF @querystore = 1
BEGIN
    DECLARE
        @db_qs    sysname,
        @cmdqs    varchar(4000),
        @cmdqs1   varchar(4000)

    -- Staging tables for query store info across all databases
    DROP TABLE IF EXISTS ##QS_Details;
    CREATE TABLE ##QS_Details
    (
        [database_name]                    sysname        NULL,
        [object_name]                      sysname        NULL,
        [query_sql_text]                   nvarchar(max)  NULL,
        [reason]                           nvarchar(4000) NULL,
        [score]                            int            NULL,
        [script]                           nvarchar(4000) NULL,
        [query_id]                         int            NULL,
        [regressedPlanId]                  int            NULL,
        [recommendedPlanId]                int            NULL,
        [regressedPlanErrorCount]          int            NULL,
        [recommendedPlanErrorCount]        int            NULL,
        [regressedPlanExecutionCount]      int            NULL,
        [regressedPlanCpuTimeAverage]      float          NULL,
        [recommendedPlanExecutionCount]    bigint         NULL,
        [recommendedPlanCpuTimeAverage]    float          NULL,
        [estimated_gain]                   float          NULL,
        [error_prone]                      varchar(3)     NOT NULL
    )

    DROP TABLE IF EXISTS ##QS
    CREATE TABLE ##QS
    (
        database_name               sysname,
        actual_state_desc           varchar(100),
        desired_state_desc          varchar(100),
        current_storage_size_mb     int,
        max_storage_size_mb         int,
        readonly_reason             int,
        flush_interval_seconds      int,
        interval_length_minutes     int,
        stale_query_threshold_days  int,
        max_plans_per_query         int,
        query_capture_mode_desc     VARCHAR(100),
        size_based_cleanup_mode_desc VARCHAR(100),
        AQT_desired_state           INT,
        AQT_Actual_state_desc       VARCHAR(100),
        AQT_reason_desc             VARCHAR(100)
    )

    -- Iterate over all databases with Query Store enabled
    DECLARE My_Cursor CURSOR LOCAL STATIC READ_ONLY FORWARD_ONLY FOR
        SELECT name FROM sys.databases WHERE is_query_store_on = 1

    OPEN My_Cursor
    FETCH NEXT FROM My_Cursor INTO @db_qs

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Collect QS configuration
        SELECT @cmdqs = 'USE [' + @db_qs + ']; '
            + 'INSERT INTO ##QS(database_name,actual_state_desc,desired_state_desc,current_storage_size_mb,'
            + 'max_storage_size_mb,readonly_reason,flush_interval_seconds,interval_length_minutes,'
            + 'stale_query_threshold_days,max_plans_per_query,query_capture_mode_desc,size_based_cleanup_mode_desc) '
            + 'SELECT ''' + @db_qs + ''' AS database_name,actual_state_desc,desired_state_desc,'
            + 'current_storage_size_mb,max_storage_size_mb,readonly_reason,flush_interval_seconds,'
            + 'interval_length_minutes,stale_query_threshold_days,max_plans_per_query,'
            + 'query_capture_mode_desc,size_based_cleanup_mode_desc FROM sys.database_query_store_options;'
        EXEC (@cmdqs)

        -- Collect automatic tuning state (FORCE_LAST_GOOD_PLAN)
        SELECT @cmdqs = 'USE [' + @db_qs + ']; '
            + 'DECLARE @AQT table(desired_state int, actual_state_desc varchar(100), reason_desc varchar(100));'
            + 'INSERT INTO @AQT SELECT desired_state, actual_state_desc, reason_desc'
            + ' FROM sys.database_automatic_tuning_options WHERE name = ''FORCE_LAST_GOOD_PLAN'';'
            + 'UPDATE ##QS SET AQT_desired_state=AQT.desired_state,'
            + 'AQT_Actual_state_desc=AQT.Actual_state_desc,AQT_reason_desc=AQT.reason_desc'
            + ' FROM @AQT AQT WHERE database_name = ''' + @db_qs + ''''
        EXEC (@cmdqs)

        -- Collect tuning recommendations
        SELECT @cmdqs1 = 'USE [' + @db_qs + ']; '
            + 'INSERT INTO ##QS_Details '
            + 'SELECT DB_NAME() AS DB_Name, OBJECT_NAME(q.object_id) AS Object_Name, qt.query_sql_text, X.* '
            + 'FROM ( '
            + '  SELECT reason, score, '
            + '    script = JSON_VALUE(details, ''$.implementationDetails.script''), '
            + '    planForceDetails.*, '
            + '    estimated_gain = (regressedPlanExecutionCount + recommendedPlanExecutionCount) '
            + '      * (regressedPlanCpuTimeAverage - recommendedPlanCpuTimeAverage)/1000000, '
            + '    error_prone = IIF(regressedPlanErrorCount > recommendedPlanErrorCount, ''YES'', ''NO'') '
            + '  FROM sys.dm_db_tuning_recommendations '
            + '  CROSS APPLY OPENJSON(Details, ''$.planForceDetails'') '
            + '    WITH ([query_id] int ''$.queryId'', regressedPlanId int ''$.regressedPlanId'', '
            + '          recommendedPlanId int ''$.recommendedPlanId'', regressedPlanErrorCount int, '
            + '          recommendedPlanErrorCount int, regressedPlanExecutionCount int, '
            + '          regressedPlanCpuTimeAverage float, recommendedPlanExecutionCount bigint, '
            + '          recommendedPlanCpuTimeAverage float) AS planForceDetails '
            + ') AS X '
            + 'INNER JOIN sys.query_store_query q ON q.query_id = X.query_id '
            + 'INNER JOIN sys.query_store_query_text qt ON qt.query_text_id = q.query_text_id'
        EXEC (@cmdqs1)

        FETCH NEXT FROM My_Cursor INTO @db_qs
    END

    CLOSE My_Cursor
    DEALLOCATE My_Cursor

    -- Return results
    SELECT @@SERVERNAME AS Server_name, *, GETUTCDATE() AS collection_time_utc FROM ##QS
    SELECT * FROM ##QS_Details ORDER BY [database_name]

    SET @ExitCondition = 1
END

IF @ExitCondition = 1 RETURN

-- ============================================================================================================================
-- MAIN ACTIVITY SECTION
-- Captures currently executing requests (running, runnable, suspended, rollback)
-- ============================================================================================================================

-- -------------------------------------------------------
-- Step 1: Capture current CPU utilization from ring buffer
-- -------------------------------------------------------
DECLARE @record_id int, @SQLProcessUtilization int, @CPU int, @EventTime datetime

SELECT TOP 1
    @record_id             = record_id,
    @SQLProcessUtilization = SQLProcessUtilization,
    @CPU                   = SQLProcessUtilization + (100 - SystemIdle - SQLProcessUtilization)
FROM (
    SELECT
        record.value('(./Record/@id)[1]', 'int')                                                      AS record_id,
        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int')            AS SystemIdle,
        record.value('(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int')    AS SQLProcessUtilization,
        timestamp
    FROM (
        SELECT timestamp, CONVERT(xml, record) AS record
        FROM sys.dm_os_ring_buffers
        WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
          AND record LIKE '%<SystemHealth>%'
    ) AS x
) AS y
ORDER BY record_id DESC

-- -------------------------------------------------------
-- Step 2: Build the main activity temp table
-- -------------------------------------------------------
DECLARE @Cmd varchar(max), @DBName sysname

SELECT
    x.session_id                AS [Sid],
    COALESCE(x.blocking_session_id, 0) AS BSid,
    @CPU                        AS CPU,
    @SQLProcessUtilization      AS SQL,
    x.Status,
    x.Start_time,
    -- Elapsed time as D.HH:MM:SS (handles multi-day sessions)
    COALESCE(
        CONVERT(varchar(5), ABS(DATEDIFF(day, (GETDATE() - x.Start_Time), '1900-01-01')))
        + ':' + CONVERT(varchar(10), (GETDATE() - x.Start_Time), 108),
        '00:00:00:00'
    )                           AS Elap_time,
    x.TotalCPU                  AS [T.CPU],
    x.totalReads                AS [T.RD],
    x.totalWrites               AS [T.WR],
    x.Writes_in_tempdb          AS [W.TDB],
    -- Current statement text (XML-safe)
    (
        SELECT SUBSTRING(text, x.statement_start_offset / 2,
            (CASE WHEN x.statement_end_offset = -1 THEN 100000000
                  ELSE x.statement_end_offset
             END - x.statement_start_offset + 3) / 2 + 1)
        FROM sys.dm_exec_sql_text(x.sql_handle)
        FOR XML PATH(''), TYPE
    )                           AS Sql_text,
    -- Database name with log % used
    DB_NAME(x.database_id) + ' (' + CAST(XD.cntr_value AS varchar(3)) + ')' AS [DBName / %LogUsed],
    SPACE(300)                  AS Object_Name,  -- placeholder; populated in cursor below
    ZZZ.dbid,
    ZZZ.objectid,
    x.Wait_type,
    x.Wait_time,
    x.Login_name,
    x.Host_name,
    -- Translate SQL Agent SPID to job name when applicable
    CASE LEFT(x.program_name, 15)
        WHEN 'SQLAgent - TSQL' THEN
            (SELECT TOP 1 'SQL Job = ' + j.name
             FROM msdb.dbo.sysjobs (NOLOCK) j
             INNER JOIN msdb.dbo.sysjobsteps (NOLOCK) s ON j.job_id = s.job_id
             WHERE RIGHT(CAST(s.job_id AS nvarchar(50)), 10) = RIGHT(SUBSTRING(x.program_name, 30, 34), 10))
            + ' - (' + SUBSTRING(x.program_name, CHARINDEX(': Step', program_name, 1) + 1, 100)
        WHEN 'SQL Server Prof' THEN 'SQL Server Profiler'
        ELSE x.program_name
    END                         AS Program_name,
    -- SQL text of the blocking session (if any)
    (
        SELECT TOP 1 p.text
        FROM (
            SELECT sql_handle, statement_start_offset, statement_end_offset
            FROM sys.dm_exec_requests r2
            WHERE r2.session_id = x.blocking_session_id
        ) AS r_blocking
        CROSS APPLY (
            SELECT SUBSTRING(text, r_blocking.statement_start_offset / 2,
                (CASE WHEN r_blocking.statement_end_offset = -1
                      THEN LEN(CONVERT(nvarchar(max), text)) * 2
                      ELSE r_blocking.statement_end_offset
                 END - r_blocking.statement_start_offset + 3) / 2)
            FROM sys.dm_exec_sql_text(r_blocking.sql_handle)
            FOR XML PATH(''), TYPE
        ) p (text)
    )                           AS blocking_text,
    -- Object name running in the blocking session
    (
        SELECT OBJECT_NAME(objectid)
        FROM sys.dm_exec_sql_text(
            (SELECT TOP 1 sql_handle FROM sys.dm_exec_requests r3 WHERE r3.session_id = x.blocking_session_id))
    )                           AS blocking_obj,
    x.percent_complete
INTO #TMP_CEXEC
FROM
(
    -- Aggregate per session (handles parallel requests)
    SELECT
        r.session_id,
        s.host_name,
        s.login_name,
        r.start_time,
        r.sql_handle,
        r.database_id,
        r.blocking_session_id,
        r.wait_type,
        r.wait_time,
        r.status,
        r.statement_start_offset,
        r.statement_end_offset,
        s.program_name,
        r.percent_complete,
        SUM(CAST(r.total_elapsed_time AS bigint)) / 1000 AS totalElapsedTime, -- CAST AS bigint: prevents overflow under high load
        SUM(CAST(r.reads  AS bigint))                    AS totalReads,
        SUM(CAST(r.writes AS bigint))                    AS totalWrites,
        SUM(CAST(r.cpu_time AS bigint))                  AS totalCPU,
        SUM(tsu.user_objects_alloc_page_count + tsu.internal_objects_alloc_page_count) AS writes_in_tempdb
    FROM sys.dm_exec_requests r
    JOIN sys.dm_exec_sessions s     ON s.session_id = r.session_id
    JOIN sys.dm_db_task_space_usage tsu ON s.session_id = tsu.session_id AND r.request_id = tsu.request_id
    WHERE r.status IN ('running', 'runnable', 'suspended', 'rollback')
    GROUP BY
        r.session_id, s.host_name, s.login_name, r.start_time, r.sql_handle, r.database_id,
        r.blocking_session_id, r.wait_type, r.wait_time, r.status,
        r.statement_start_offset, r.statement_end_offset, s.program_name, r.percent_complete
) x
OUTER APPLY sys.dm_exec_sql_text(x.sql_handle) AS ZZZ
-- Log percentage used for the session's database
LEFT JOIN (
    SELECT instance_name, cntr_value
    FROM sys.dm_os_performance_counters
    WHERE object_name  LIKE '%Databases%'
      AND counter_name = 'Percent Log Used'
) XD ON XD.instance_name = DB_NAME(x.database_id)
WHERE x.session_id <> @@SPID
  AND (
        @IncludeSystem = 1
        OR x.wait_type NOT IN ('WAITFOR', 'SP_SERVER_DIAGNOSTICS_SLEEP', 'TRACEWRITE', 'BROKER_RECEIVE_WAITFOR')
        OR x.wait_type IS NULL
      )
ORDER BY x.totalCPU DESC

-- Indexes to speed up subsequent lookups
CREATE INDEX X1 ON #TMP_CEXEC ([Object_name]);
CREATE INDEX X2 ON #TMP_CEXEC (Elap_time);

-- -------------------------------------------------------
-- Step 3: Fix elapsed time overflow (23:59:59 artifact)
-- -------------------------------------------------------
UPDATE #TMP_CEXEC
SET Elap_time = '0.00:00:00'
WHERE Elap_time LIKE '%23:59:59%'

-- -------------------------------------------------------
-- Step 4: Resolve object names via cursor
--         Update Object_Name from the object's home database
-- -------------------------------------------------------
DECLARE curObj CURSOR FOR
    SELECT DISTINCT [dbid], DB_NAME([dbid]), objectid
    FROM #TMP_CEXEC
    WHERE dbid IS NOT NULL

DECLARE @DBID2 int, @objid int
SET @DBName = NULL

OPEN curObj
WHILE (1 = 1)
BEGIN
    FETCH NEXT FROM curObj INTO @DBID2, @DBName, @objid
    IF @@fetch_status <> 0 BREAK

    -- Use sys.objects + sys.schemas to get schema-qualified name
    SET @Cmd = 'UPDATE T SET Object_name = SC.name + ''.'' + O.name '
             + 'FROM #TMP_CEXEC T '
             + 'INNER JOIN [' + @DBName + '].sys.objects  O  (NOLOCK) ON O.object_id  = T.objectid '
             + 'INNER JOIN [' + @DBName + '].sys.schemas SC  (NOLOCK) ON O.schema_id  = SC.schema_id '
             + 'WHERE T.dbid = ' + CONVERT(varchar, @DBID2)
    EXEC (@Cmd)
END
CLOSE curObj
DEALLOCATE curObj

-- -------------------------------------------------------
-- Step 5: Identify blocking chains
--         Blocking sessions get a negated SID for visual flagging
-- -------------------------------------------------------
DECLARE @BlockingSID TABLE (BSID int)
INSERT INTO @BlockingSID (BSID)
SELECT DISTINCT BSID FROM #TMP_CEXEC WHERE BSID > 0

-- Negate SID of sessions that are also blockers (visual indicator)
UPDATE X
SET [SID] = -1 * [SID]
FROM #TMP_CEXEC X
INNER JOIN @BlockingSID B ON X.[SID] = B.[BSID]

-- Collect SPIDs involved in blocking (blocked + blockers)
DECLARE @SPIDB TABLE (SPID int, BSPID int)
DELETE FROM @SPIDB

INSERT INTO @SPIDB (SPID)
SELECT DISTINCT * FROM (
    SELECT DISTINCT [Sid] FROM #TMP_CEXEC WHERE BSid > 0
    UNION
    SELECT DISTINCT  BSid FROM #TMP_CEXEC WHERE BSid > 0
    UNION
    SELECT BSID FROM @BlockingSID
) X

-- ============================================================================================================================
-- FILTER MODE: When @Filter_Name is provided, apply it and return filtered result set only
-- ============================================================================================================================
IF @Filter_Name IS NOT NULL
BEGIN
    IF @Filter_Name NOT IN ('SPID', 'Program_Name', 'Login_Name', 'Host_Name', 'Object_name')
    BEGIN
        SELECT 'Valid @Filter_Name values: SPID, Program_Name, Login_Name, Host_Name, Object_name' AS [Error]
        SET @Filter_Name = NULL
    END
    ELSE
    BEGIN
        -- Build a normalized result set then apply the filter
        SELECT
            CASE
                WHEN COALESCE(BSid, 0) > 0 AND percent_complete = 0
                    THEN CAST([Sid] AS varchar(5)) + '(' + CAST(BSid AS varchar(5)) + ') *'
                WHEN COALESCE(BSid, 0) = 0 AND percent_complete = 0
                    THEN CAST([Sid] AS varchar(5))
                WHEN COALESCE(BSid, 0) > 0 AND percent_complete <> 0
                    THEN CAST([Sid] AS varchar(5)) + '(' + CAST(BSid AS varchar(5)) + ') // PC= ' + CAST(ROUND(percent_complete, 3) AS varchar(10)) + '% *'
                WHEN COALESCE(BSid, 0) = 0 AND percent_complete <> 0
                    THEN CAST([Sid] AS varchar(5)) + ' // PC= ' + CAST(ROUND(percent_complete, 3) AS varchar(10)) + '%'
            END                              AS SPID,
            CAST(CPU AS varchar(3)) + '/' + CAST([SQL] AS varchar(3)) AS [CPU/SQL],
            [Status],
            Start_Time,
            Elap_time,
            SQL_Text,
            [DBName / %LogUsed],
            object_name,
            Wait_Type,
            Wait_Time,
            Program_Name,
            Host_name,
            Login_name,
            [T.CPU], [T.RD], [T.WR], [W.TDB],
            blocking_text,
            blocking_obj
        INTO #TMP_CEXEC_FILTER
        FROM #TMP_CEXEC

        SELECT @Cmd = 'SELECT * FROM #TMP_CEXEC_FILTER WHERE ' + @Filter_Name + ' LIKE ''%' + @Filter_Value + '%'' ORDER BY [Start_Time]'
        EXEC (@Cmd)
        RETURN
    END
END

-- ============================================================================================================================
-- EXCEL MODE: Clean XML encoding for paste to Excel
-- ============================================================================================================================
IF @excel = 1
BEGIN
    SELECT
        CASE
            WHEN COALESCE([Sid], 0) < 0 AND COALESCE(BSid, 0) > 0 THEN CAST(ABS([Sid]) AS varchar(5)) + '(' + CAST(BSid AS varchar(5)) + ') *'
            WHEN COALESCE([Sid], 0) < 0 AND COALESCE(BSid, 0) = 0 THEN CAST(ABS([Sid]) AS varchar(5)) + ' *'
            WHEN COALESCE(BSid, 0) > 0 AND percent_complete = 0   THEN CAST([Sid] AS varchar(5)) + '(' + CAST(BSid AS varchar(5)) + ') *'
            WHEN COALESCE(BSid, 0) = 0 AND percent_complete = 0   THEN CAST([Sid] AS varchar(5))
            WHEN COALESCE(BSid, 0) > 0 AND percent_complete <> 0  THEN CAST([Sid] AS varchar(5)) + '(' + CAST(BSid AS varchar(5)) + ') // PC= ' + CAST(ROUND(percent_complete, 3) AS varchar(10)) + '% *'
            WHEN COALESCE(BSid, 0) = 0 AND percent_complete <> 0  THEN CAST([Sid] AS varchar(5)) + ' // PC= ' + CAST(ROUND(percent_complete, 3) AS varchar(10)) + '%'
        END                              AS SPID,
        CAST(CPU AS varchar(3)) + '/' + CAST([SQL] AS varchar(3)) AS [CPU/SQL],
        [Status],
        Start_Time,
        Elap_time,
        -- Strip XML entities for Excel readability
        '"' + REPLACE(REPLACE(REPLACE(CAST(SQL_Text AS varchar(max)), '&#x0D;', ' '), '&lt;', '<'), '&gt;', '>') + '"' AS SQL_Text,
        [DBName / %LogUsed],
        object_name,
        Wait_Type, Wait_Time,
        Program_Name, Host_name, Login_name,
        [T.CPU], [T.RD], [T.WR], [W.TDB],
        blocking_text, blocking_obj
    FROM #TMP_CEXEC
    ORDER BY
        CASE WHEN @OrderBy = 'Start_Time'   THEN Start_Time END ASC,
        CASE WHEN @OrderBy = 'Wait_Time'    THEN Wait_Time  END DESC,
        CASE WHEN @OrderBy = 'Elap_Time'    THEN Elap_Time  END DESC,
        CASE WHEN @OrderBy = 'Status'       THEN [Status]   END DESC,
        CASE WHEN @OrderBy = 'DB'           THEN [DBName / %LogUsed] END ASC,
        CASE WHEN @OrderBy = 'T.RD'         THEN [T.RD]  END DESC,
        CASE WHEN @OrderBy = 'T.WR'         THEN [T.WR]  END DESC,
        CASE WHEN @OrderBy IN ('W.TDB','TempDB') THEN [W.TDB] END DESC,
        CASE WHEN @OrderBy IN ('T.CPU','CPU')    THEN [T.CPU] END DESC,
        CASE WHEN @OrderBy = 'Login_Name'   THEN Login_Name   END ASC,
        CASE WHEN @OrderBy = 'Host_Name'    THEN [Host_Name]  END ASC,
        CASE WHEN @OrderBy = 'Program_Name' THEN [Program_Name] END ASC,
        CASE WHEN @OrderBy = 'Object_Name'  THEN [Object_Name] END ASC,
        CASE WHEN @OrderBy IN ('SPID','SPD') THEN [SID] ELSE [Start_Time] END
    RETURN
END

-- ============================================================================================================================
-- ALL ACTIVITY MODE (@Blocks = 0): Return all executing sessions
-- ============================================================================================================================
IF @Blocks = 0
BEGIN
    SELECT
        CASE
            WHEN COALESCE([Sid], 0) < 0 AND COALESCE(BSid, 0) > 0 THEN CAST(ABS([Sid]) AS varchar(5)) + '(' + CAST(BSid AS varchar(5)) + ') *'
            WHEN COALESCE([Sid], 0) < 0 AND COALESCE(BSid, 0) = 0 THEN CAST(ABS([Sid]) AS varchar(5)) + ' *'
            WHEN COALESCE(BSid, 0) > 0 AND percent_complete = 0   THEN CAST([Sid] AS varchar(5)) + '(' + CAST(BSid AS varchar(5)) + ') *'
            WHEN COALESCE(BSid, 0) = 0 AND percent_complete = 0   THEN CAST([Sid] AS varchar(5))
            WHEN COALESCE(BSid, 0) > 0 AND percent_complete <> 0  THEN CAST([Sid] AS varchar(5)) + '(' + CAST(BSid AS varchar(5)) + ') // PC= ' + CAST(ROUND(percent_complete, 3) AS varchar(10)) + '% *'
            WHEN COALESCE(BSid, 0) = 0 AND percent_complete <> 0  THEN CAST([Sid] AS varchar(5)) + ' // PC= ' + CAST(ROUND(percent_complete, 3) AS varchar(10)) + '%'
        END                              AS SPID,
        CAST(CPU AS varchar(3)) + '/' + CAST([SQL] AS varchar(3)) AS [CPU/SQL],
        [Status],
        Start_Time, Elap_time, SQL_Text,
        [DBName / %LogUsed], object_name,
        Wait_Type, Wait_Time,
        Program_Name, Host_name, Login_name,
        [T.CPU], [T.RD], [T.WR], [W.TDB],
        blocking_text, blocking_obj
    FROM #TMP_CEXEC
    ORDER BY
        CASE WHEN @OrderBy = 'Start_Time'   THEN Start_Time END ASC,
        CASE WHEN @OrderBy = 'Wait_Time'    THEN Wait_Time  END DESC,
        CASE WHEN @OrderBy = 'Elap_Time'    THEN Elap_Time  END DESC,
        CASE WHEN @OrderBy = 'Status'       THEN [Status]   END DESC,
        CASE WHEN @OrderBy = 'DB'           THEN [DBName / %LogUsed] END ASC,
        CASE WHEN @OrderBy = 'T.RD'         THEN [T.RD]  END DESC,
        CASE WHEN @OrderBy = 'T.WR'         THEN [T.WR]  END DESC,
        CASE WHEN @OrderBy IN ('W.TDB','TempDB') THEN [W.TDB] END DESC,
        CASE WHEN @OrderBy IN ('T.CPU','CPU')    THEN [T.CPU] END DESC,
        CASE WHEN @OrderBy = 'Login_Name'   THEN Login_Name   END ASC,
        CASE WHEN @OrderBy = 'Host_Name'    THEN [Host_Name]  END ASC,
        CASE WHEN @OrderBy = 'Program_Name' THEN [Program_Name] END ASC,
        CASE WHEN @OrderBy = 'Object_Name'  THEN [Object_Name] END ASC,
        CASE WHEN @OrderBy IN ('SPID','SPD') THEN [SID] ELSE [Start_Time] END
END

-- ============================================================================================================================
-- BLOCKING MODE (@MultiServer = 0): Return blocking chain detail (only when blocking exists)
-- ============================================================================================================================
IF @MultiServer = 0
BEGIN
    IF EXISTS (SELECT 1 FROM #TMP_CEXEC WHERE BSid <> 0)
    BEGIN
        SELECT
            CASE
                WHEN COALESCE(MA.[Sid], 0) < 0 AND COALESCE(MA.BSid, 0) > 0 THEN CAST(ABS(MA.[Sid]) AS varchar(5)) + '(' + CAST(MA.BSid AS varchar(5)) + ') *'
                WHEN COALESCE(MA.[Sid], 0) < 0 AND COALESCE(MA.BSid, 0) = 0 THEN CAST(ABS(MA.[Sid]) AS varchar(5))
                WHEN COALESCE(MA.BSid, 0) > 0 AND percent_complete = 0       THEN CAST(MA.[Sid] AS varchar(5)) + '(' + CAST(MA.BSid AS varchar(5)) + ') *'
                WHEN COALESCE(MA.BSid, 0) = 0 AND percent_complete = 0       THEN CAST(MA.[Sid] AS varchar(5))
                WHEN COALESCE(MA.BSid, 0) > 0 AND percent_complete <> 0      THEN CAST(MA.[Sid] AS varchar(5)) + '(' + CAST(MA.BSid AS varchar(5)) + ') // PC= ' + CAST(ROUND(percent_complete, 3) AS varchar(10)) + '% *'
                WHEN COALESCE(MA.BSid, 0) = 0 AND percent_complete <> 0      THEN CAST(MA.[Sid] AS varchar(5)) + ' // PC= ' + CAST(ROUND(percent_complete, 3) AS varchar(10)) + '%'
            END                              AS SPID,
            T2.[# Blocking],
            NULLIF(MA.[BSid], 0)             AS [BSid],
            CAST(CPU AS varchar(3)) + '/' + CAST([SQL] AS varchar(3)) AS [CPU/SQL],
            [Status],
            Start_Time, Elap_time, SQL_Text,
            [DBName / %LogUsed], object_name,
            Wait_Type, Wait_Time,
            Host_name, Program_Name, Login_name,
            [T.CPU], [T.RD], [T.WR], [W.TDB]
        FROM #TMP_CEXEC MA
        INNER JOIN @SPIDB D1 ON ABS(MA.[SID]) = D1.SPID
        LEFT JOIN (
            SELECT X.BSid, COUNT(*) AS [# Blocking]
            FROM #TMP_CEXEC X
            WHERE bsid <> 0
            GROUP BY BSid
        ) T2 ON T2.bsid = ABS(MA.[sid])
        WHERE MA.BSid <> 0 OR T2.BSid IS NOT NULL
        ORDER BY Elap_time DESC
    END
END

-- ============================================================================================================================
-- MULTI-SERVER BLOCKING MODE (@MultiServer = 1): Blocking chain only (for aggregation across servers)
-- ============================================================================================================================
IF @MultiServer = 1
BEGIN
    SELECT
        CASE
            WHEN COALESCE(MA.[Sid], 0) < 0 AND COALESCE(MA.BSid, 0) > 0 THEN CAST(ABS(MA.[Sid]) AS varchar(5)) + '(' + CAST(MA.BSid AS varchar(5)) + ') *'
            WHEN COALESCE(MA.[Sid], 0) < 0 AND COALESCE(MA.BSid, 0) = 0 THEN CAST(ABS(MA.[Sid]) AS varchar(5))
            WHEN COALESCE(MA.BSid, 0) > 0 AND percent_complete = 0       THEN CAST(MA.[Sid] AS varchar(5)) + '(' + CAST(MA.BSid AS varchar(5)) + ') *'
            WHEN COALESCE(MA.BSid, 0) = 0 AND percent_complete = 0       THEN CAST(MA.[Sid] AS varchar(5))
            WHEN COALESCE(MA.BSid, 0) > 0 AND percent_complete <> 0      THEN CAST(MA.[Sid] AS varchar(5)) + '(' + CAST(MA.BSid AS varchar(5)) + ') // PC= ' + CAST(ROUND(percent_complete, 3) AS varchar(10)) + '% *'
            WHEN COALESCE(MA.BSid, 0) = 0 AND percent_complete <> 0      THEN CAST(MA.[Sid] AS varchar(5)) + ' // PC= ' + CAST(ROUND(percent_complete, 3) AS varchar(10)) + '%'
        END                              AS SPID,
        T2.[# Blocking],
        NULLIF(MA.[BSid], 0)             AS [BSid],
        CAST(CPU AS varchar(3)) + '/' + CAST([SQL] AS varchar(3)) AS [CPU/SQL],
        [Status],
        Start_Time, Elap_time, SQL_Text,
        [DBName / %LogUsed], object_name,
        Wait_Type, Wait_Time,
        Host_name, Program_Name, Login_name,
        [T.CPU], [T.RD], [T.WR], [W.TDB]
    FROM #TMP_CEXEC MA
    INNER JOIN @SPIDB D1 ON MA.[SID] = D1.SPID
    LEFT JOIN (
        SELECT X.BSid, COUNT(*) AS [# Blocking]
        FROM #TMP_CEXEC X
        WHERE bsid <> 0
        GROUP BY BSid
    ) T2 ON T2.bsid = ABS(MA.sid)
    WHERE MA.BSid <> 0 OR T2.BSid IS NOT NULL
    ORDER BY Elap_time DESC
END

GO