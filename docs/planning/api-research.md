# dbxtop API Research: Comprehensive Endpoint Inventory

> **Date:** 2026-03-16
> **Purpose:** Document every API endpoint available for building a real-time terminal dashboard for Databricks/Spark clusters.
> **Scope:** Databricks Python SDK, Databricks REST API, Spark REST API v1, Spark Metrics System

---

## Table of Contents

1. [Databricks Python SDK Endpoints](#1-databricks-python-sdk-endpoints)
   - [Clusters API](#11-clusters-api-clustersext)
   - [Jobs API](#12-jobs-api)
   - [Command Execution API](#13-command-execution-api)
   - [Libraries API](#14-libraries-api)
   - [SQL Warehouses API](#15-sql-warehouses-api)
   - [SQL Statement Execution API](#16-sql-statement-execution-api)
   - [SQL Query History API](#17-sql-query-history-api)
   - [Delta Live Tables / Pipelines API](#18-delta-live-tables--pipelines-api)
   - [Instance Pools API](#19-instance-pools-api)
   - [Cluster Policies API](#110-cluster-policies-api)
   - [Serving Endpoints API](#111-serving-endpoints-api)
   - [MLflow Experiments API](#112-mlflow-experiments-api)
   - [Token Management API](#113-token-management-api)
   - [Global Init Scripts API](#114-global-init-scripts-api)
2. [Spark REST API v1 Endpoints](#2-spark-rest-api-v1-endpoints)
   - [Applications](#21-applications)
   - [Jobs](#22-jobs)
   - [Stages](#23-stages)
   - [Executors](#24-executors)
   - [Storage / RDD](#25-storage--rdd)
   - [Environment](#26-environment)
   - [SQL](#27-sql)
   - [Streaming](#28-streaming)
   - [Event Logs](#29-event-logs)
   - [Metrics / Prometheus](#210-metrics--prometheus)
   - [Version](#211-version)
3. [Spark Metrics System](#3-spark-metrics-system)
4. [Authentication & Configuration](#4-authentication--configuration)
5. [Spark Application ID Discovery](#5-spark-application-id-discovery)
6. [Databricks Driver Proxy API](#6-databricks-driver-proxy-api)
7. [Rate Limits & Performance Considerations](#7-rate-limits--performance-considerations)
8. [Gaps: What's in the UI but NOT in the API](#8-gaps-whats-in-the-ui-but-not-in-the-api)
9. [Shared vs Single-User Cluster Availability](#9-shared-vs-single-user-cluster-availability)
10. [Recommendations for dbxtop](#10-recommendations-for-dbxtop)

---

## 1. Databricks Python SDK Endpoints

**Package:** `databricks-sdk` (pip install databricks-sdk)
**Client:** `from databricks.sdk import WorkspaceClient`

### 1.1 Clusters API (`ClustersExt`)

**Access:** `w = WorkspaceClient(); w.clusters.<method>()`
**REST Base:** `/api/2.0/clusters/` and `/api/2.1/clusters/`

| Method | Parameters | Return Type | REST Path | Description |
|--------|-----------|-------------|-----------|-------------|
| `get` | `cluster_id: str` | `ClusterDetails` | `GET /api/2.0/clusters/get` | Full cluster state, config, nodes, memory |
| `list` | `filter_by?, page_size?, page_token?, sort_by?` | `Iterator[ClusterDetails]` | `GET /api/2.0/clusters/list` | All active/pinned clusters (last 30 days) |
| `events` | `cluster_id, end_time?, event_types?, offset?, limit?, order?, start_time?` | `Iterator[ClusterEvent]` | `POST /api/2.0/clusters/events` | Cluster lifecycle events (resize, terminate, etc.) |
| `start` | `cluster_id: str` | `Wait[ClusterDetails]` | `POST /api/2.0/clusters/start` | Start terminated cluster |
| `start_and_wait` | `cluster_id, timeout?` | `ClusterDetails` | — | Start + poll until RUNNING |
| `restart` | `cluster_id, restart_user?` | `Wait[ClusterDetails]` | `POST /api/2.0/clusters/restart` | Restart running cluster |
| `restart_and_wait` | `cluster_id, timeout?` | `ClusterDetails` | — | Restart + poll |
| `delete` | `cluster_id: str` | `Wait[ClusterDetails]` | `POST /api/2.0/clusters/delete` | Terminate cluster |
| `delete_and_wait` | `cluster_id, timeout?` | `ClusterDetails` | — | Terminate + poll until TERMINATED |
| `resize` | `cluster_id, autoscale? or num_workers?` | `Wait[ClusterDetails]` | `POST /api/2.0/clusters/resize` | Change worker count |
| `resize_and_wait` | `cluster_id, autoscale?, num_workers?, timeout?` | `ClusterDetails` | — | Resize + poll |
| `create` | `spark_version + 30+ config params` | `Wait[ClusterDetails]` | `POST /api/2.0/clusters/create` | Provision new cluster |
| `create_and_wait` | same + `timeout` | `ClusterDetails` | — | Create + poll |
| `edit` | `cluster_id, spark_version + 30+ params` | `Wait[ClusterDetails]` | `POST /api/2.0/clusters/edit` | Update cluster config |
| `edit_and_wait` | same + `timeout` | `ClusterDetails` | — | Edit + poll |
| `update` | `cluster_id, update_mask, cluster?` | `Wait[ClusterDetails]` | `PATCH /api/2.1/clusters/update` | Partial config update |
| `update_and_wait` | same + `timeout` | `ClusterDetails` | — | Update + poll |
| `permanent_delete` | `cluster_id: str` | `None` | `POST /api/2.0/clusters/permanent-delete` | Permanently delete |
| `pin` | `cluster_id: str` | `None` | `POST /api/2.0/clusters/pin` | Pin cluster in list |
| `unpin` | `cluster_id: str` | `None` | `POST /api/2.0/clusters/unpin` | Unpin cluster |
| `change_owner` | `cluster_id, owner_username` | `None` | `POST /api/2.0/clusters/change-owner` | Transfer ownership |
| `ensure_cluster_is_running` | `cluster_id: str` | `None` | — | Helper: start if not running |
| `spark_versions` | — | `GetSparkVersionsResponse` | `GET /api/2.0/clusters/spark-versions` | Available DBR versions |
| `list_node_types` | — | `ListNodeTypesResponse` | `GET /api/2.0/clusters/list-node-types` | Available VM/instance types |
| `list_zones` | — | `ListAvailableZonesResponse` | `GET /api/2.0/clusters/list-zones` | Available cloud zones |
| `get_permissions` | `cluster_id` | `ClusterPermissions` | `GET /api/2.0/permissions/clusters/{id}` | Cluster ACLs |
| `get_permission_levels` | `cluster_id` | `GetClusterPermissionLevelsResponse` | `GET /api/2.0/permissions/clusters/{id}/permissionLevels` | Available permission levels |
| `set_permissions` | `cluster_id, access_control_list` | `ClusterPermissions` | `PUT /api/2.0/permissions/clusters/{id}` | Replace permissions |
| `update_permissions` | `cluster_id, access_control_list` | `ClusterPermissions` | `PATCH /api/2.0/permissions/clusters/{id}` | Modify permissions |
| `wait_get_cluster_running` | `cluster_id, timeout?, callback?` | `ClusterDetails` | — | Poll until RUNNING |
| `wait_get_cluster_terminated` | `cluster_id, timeout?, callback?` | `ClusterDetails` | — | Poll until TERMINATED |

#### ClusterDetails Key Fields (for dashboard display)

| Field | Type | Dashboard Relevance |
|-------|------|-------------------|
| `cluster_id` | `str` | Primary identifier |
| `cluster_name` | `str` | Display name |
| `state` | `State` enum | **CRITICAL** — PENDING, RUNNING, RESTARTING, RESIZING, TERMINATING, TERMINATED, ERROR |
| `state_message` | `str` | Human-readable state context |
| `num_workers` | `int` | Current worker count |
| `autoscale` | `AutoScale` | min_workers, max_workers |
| `cluster_cores` | `float` | **Total CPU cores across all nodes** |
| `cluster_memory_mb` | `int` | **Total memory in MB** |
| `spark_version` | `str` | DBR version |
| `node_type_id` | `str` | Worker VM type |
| `driver_node_type_id` | `str` | Driver VM type |
| `driver` | `SparkNode` | Driver node details (host_private_ip, instance_id, start_timestamp) |
| `executors` | `List[SparkNode]` | **Worker node list** with individual details |
| `start_time` | `int` (epoch ms) | Cluster start time |
| `terminated_time` | `int` (epoch ms) | Termination time |
| `last_restarted_time` | `int` (epoch ms) | Last restart |
| `last_state_loss_time` | `int` (epoch ms) | Last driver failure |
| `spark_context_id` | `int` | Changes on driver restart |
| `jdbc_port` | `int` | JDBC port on driver |
| `termination_reason` | `TerminationReason` | code + parameters explaining shutdown |
| `creator_user_name` | `str` | Who created the cluster |
| `cluster_source` | `ClusterSource` | API, JOB, MODELS, PIPELINE, SQL, UI |
| `spark_conf` | `Dict[str,str]` | Spark configuration |
| `custom_tags` | `Dict[str,str]` | User tags |
| `default_tags` | `Dict[str,str]` | System tags |
| `cluster_log_status` | `LogSyncStatus` | Log delivery status |
| `data_security_mode` | `DataSecurityMode` | NONE, SINGLE_USER, USER_ISOLATION, etc. |
| `runtime_engine` | `RuntimeEngine` | STANDARD or PHOTON |
| `autotermination_minutes` | `int` | Auto-shutdown timeout |
| `policy_id` | `str` | Applied cluster policy |
| `enable_elastic_disk` | `bool` | Dynamic storage expansion |

#### ClusterEvent Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `cluster_id` | `str` | Cluster identifier |
| `type` | `EventType` | CREATING, STARTING, RESTARTING, TERMINATING, RESIZING, UPSIZE_COMPLETED, NODES_LOST, DRIVER_HEALTHY, DRIVER_NOT_RESPONDING, DBFS_DOWN, METASTORE_DOWN, AUTOSCALING_STATS_REPORT, SPARK_EXCEPTION, etc. |
| `timestamp` | `int` (epoch ms) | When event occurred |
| `details` | `EventDetails` | Event-specific payload (cause, reason, current/target workers, etc.) |

### 1.2 Jobs API

**Access:** `w.jobs.<method>()`
**REST Base:** `/api/2.1/jobs/`

| Method | Parameters | Return Type | REST Path | Description |
|--------|-----------|-------------|-----------|-------------|
| `list` | `expand_tasks?, limit?, name?, offset?, page_token?` | `Iterator[BaseJob]` | `GET /api/2.1/jobs/list` | All jobs with pagination |
| `get` | `job_id: int, page_token?` | `Job` | `GET /api/2.1/jobs/get` | Full job details with all tasks |
| `list_runs` | `active_only?, completed_only?, expand_tasks?, job_id?, limit?, offset?, page_token?, run_type?, start_time_from?, start_time_to?` | `Iterator[BaseRun]` | `GET /api/2.1/jobs/runs/list` | **Job runs sorted by start time** |
| `get_run` | `run_id: int, include_history?, include_resolved_values?, page_token?` | `Run` | `GET /api/2.1/jobs/runs/get` | **Detailed run metadata + tasks** |
| `get_run_output` | `run_id: int` | `RunOutput` | `GET /api/2.1/jobs/runs/get-output` | Notebook task output (max 5 MB) |
| `cancel_run` | `run_id: int` | `Wait[Run]` | `POST /api/2.1/jobs/runs/cancel` | Cancel active run |
| `cancel_all_runs` | `all_queued_runs?, job_id?` | `None` | `POST /api/2.1/jobs/runs/cancel-all` | Cancel all active runs for a job |
| `repair_run` | `run_id + task/param overrides` | `Wait[Run]` | `POST /api/2.1/jobs/runs/repair` | Re-run failed tasks |
| `delete_run` | `run_id: int` | `None` | `POST /api/2.1/jobs/runs/delete` | Delete non-active run |
| `export_run` | `run_id, views_to_export?` | `ExportRunOutput` | `GET /api/2.1/jobs/runs/export` | Export run views (CODE, DASHBOARDS, ALL) |
| `create` | `name + tasks + schedule + ...` | `CreateResponse` | `POST /api/2.1/jobs/create` | Create new job |
| `update` | `job_id + new_settings` | `None` | `POST /api/2.1/jobs/update` | Update job config |
| `reset` | `job_id, new_settings` | `None` | `POST /api/2.1/jobs/reset` | Overwrite job settings |
| `delete` | `job_id: int` | `None` | `POST /api/2.1/jobs/delete` | Delete job |
| `run_now` | `job_id + param overrides` | `Wait[Run]` | `POST /api/2.1/jobs/run-now` | Trigger immediate run |
| `submit` | `tasks + run_name + ...` | `Wait[Run]` | `POST /api/2.1/jobs/runs/submit` | One-time run without creating a job |

#### Key Run Fields for Dashboard

| Field | Type | Description |
|-------|------|-------------|
| `run_id` | `int` | Run identifier |
| `job_id` | `int` | Parent job |
| `state` | `RunState` | life_cycle_state (PENDING, RUNNING, TERMINATING, TERMINATED, SKIPPED, INTERNAL_ERROR), result_state (SUCCESS, FAILED, TIMEDOUT, CANCELED, MAXIMUM_CONCURRENT_RUNS_REACHED), state_message |
| `start_time` | `int` (epoch ms) | Run start |
| `end_time` | `int` (epoch ms) | Run end |
| `setup_duration` | `int` (ms) | Cluster setup time |
| `execution_duration` | `int` (ms) | Task execution time |
| `cleanup_duration` | `int` (ms) | Cleanup time |
| `tasks` | `List[RunTask]` | Individual task runs with their states |
| `cluster_instance` | `ClusterInstance` | cluster_id, spark_context_id |
| `run_type` | `RunType` | JOB_RUN, WORKFLOW_RUN, SUBMIT_RUN |
| `trigger` | `TriggerType` | PERIODIC, ONE_TIME, RETRY, etc. |
| `creator_user_name` | `str` | Who triggered |

### 1.3 Command Execution API

**Access:** `w.command_execution.<method>()`
**REST Base:** `/api/1.2/`

| Method | Parameters | Return Type | Description |
|--------|-----------|-------------|-------------|
| `create` | `cluster_id?, language?` | `Wait[ContextStatusResponse]` | Create execution context |
| `create_and_wait` | `cluster_id?, language?, timeout?` | `ContextStatusResponse` | Create + wait |
| `execute` | `cluster_id?, command?, context_id?, language?` | `Wait[CommandStatusResponse]` | Execute code in context |
| `execute_and_wait` | `cluster_id?, command?, context_id?, language?, timeout?` | `CommandStatusResponse` | Execute + wait for result |
| `command_status` | `cluster_id, context_id, command_id` | `CommandStatusResponse` | **Poll command status** |
| `context_status` | `cluster_id, context_id` | `ContextStatusResponse` | **Poll context status** |
| `cancel` | `cluster_id?, command_id?, context_id?` | `Wait[CommandStatusResponse]` | Cancel running command |
| `cancel_and_wait` | same + `timeout` | `CommandStatusResponse` | Cancel + wait |
| `destroy` | `cluster_id, context_id` | `None` | Remove execution context |

**Dashboard use:** Can execute arbitrary Spark commands to collect metrics not available via REST API (e.g., `spark.sparkContext.statusTracker`).

### 1.4 Libraries API

**Access:** `w.libraries.<method>()`
**REST Base:** `/api/2.0/libraries/`

| Method | Parameters | Return Type | REST Path | Description |
|--------|-----------|-------------|-----------|-------------|
| `all_cluster_statuses` | — | `Iterator[ClusterLibraryStatuses]` | `GET /api/2.0/libraries/all-cluster-statuses` | **Library status across all clusters** |
| `cluster_status` | `cluster_id: str` | `Iterator[LibraryFullStatus]` | `GET /api/2.0/libraries/cluster-status` | **Library status on specific cluster** |
| `install` | `cluster_id, libraries` | `None` | `POST /api/2.0/libraries/install` | Install libraries |
| `uninstall` | `cluster_id, libraries` | `None` | `POST /api/2.0/libraries/uninstall` | Uninstall libraries |

#### LibraryFullStatus Fields

| Field | Type | Description |
|-------|------|-------------|
| `library` | `Library` | Library spec (pypi, maven, jar, etc.) |
| `status` | `str` | PENDING, RESOLVING, INSTALLING, INSTALLED, FAILED, UNINSTALL_ON_RESTART |
| `messages` | `List[str]` | Status messages / errors |
| `is_library_for_all_clusters` | `bool` | Global library flag |

### 1.5 SQL Warehouses API

**Access:** `w.warehouses.<method>()`
**REST Base:** `/api/2.0/sql/warehouses/`

| Method | Parameters | Return Type | Description |
|--------|-----------|-------------|-------------|
| `list` | `page_size?, page_token?, run_as_user_id?` | `Iterator[EndpointInfo]` | All accessible SQL warehouses |
| `get` | `id: str` | `GetWarehouseResponse` | **Warehouse details + state** |
| `start` | `id: str` | `Wait[GetWarehouseResponse]` | Start stopped warehouse |
| `start_and_wait` | `id, timeout?` | `GetWarehouseResponse` | Start + poll |
| `stop` | `id: str` | `Wait[None]` | Stop warehouse |
| `stop_and_wait` | `id, timeout?` | `GetWarehouseResponse` | Stop + poll |
| `create` | `name + config` | `Wait[GetWarehouseResponse]` | Create warehouse |
| `edit` | `id + config` | `Wait[GetWarehouseResponse]` | Update warehouse |
| `delete` | `id: str` | `None` | Delete warehouse |
| `wait_get_warehouse_running` | `id, timeout?, callback?` | `GetWarehouseResponse` | Poll until RUNNING |
| `wait_get_warehouse_stopped` | `id, timeout?, callback?` | `GetWarehouseResponse` | Poll until STOPPED |
| `get_permissions` | `warehouse_id` | `WarehousePermissions` | Warehouse ACLs |
| `get_permission_levels` | `warehouse_id` | `GetWarehousePermissionLevelsResponse` | Permission levels |
| `set_permissions` | `warehouse_id, acl` | `WarehousePermissions` | Replace permissions |
| `update_permissions` | `warehouse_id, acl` | `WarehousePermissions` | Modify permissions |

### 1.6 SQL Statement Execution API

**Access:** `w.statement_execution.<method>()`
**REST Base:** `/api/2.0/sql/statements/`

| Method | Parameters | Return Type | Description |
|--------|-----------|-------------|-------------|
| `execute_statement` | `statement, warehouse_id, byte_limit?, catalog?, disposition?, format?, on_wait_timeout?, parameters?, query_tags?, row_limit?, schema?, wait_timeout?` | `StatementResponse` | Execute SQL (sync/async) |
| `get_statement` | `statement_id: str` | `StatementResponse` | **Poll statement status + results** |
| `get_statement_result_chunk_n` | `statement_id, chunk_index` | `ResultData` | Fetch result chunk by index |
| `cancel_execution` | `statement_id: str` | `None` | Cancel running statement |

**Result formats:** JSON_ARRAY, ARROW_STREAM, CSV. Max query size: 16 MiB.

### 1.7 SQL Query History API

**Access:** `w.query_history.<method>()`
**REST Base:** `/api/2.0/sql/history/queries`

| Method | Parameters | Return Type | Description |
|--------|-----------|-------------|-------------|
| `list` | `filter_by?, include_metrics?, max_results?, page_token?` | `ListQueriesResponse` | **Query execution history** with optional metrics |

**Filters:** user_id, warehouse_id, status, time range. Results sorted most-recent-first.

### 1.8 Delta Live Tables / Pipelines API

**Access:** `w.pipelines.<method>()`
**REST Base:** `/api/2.0/pipelines/`

| Method | Parameters | Return Type | Description |
|--------|-----------|-------------|-------------|
| `list_pipelines` | `filter?, max_results?, order_by?, page_token?` | `Iterator[PipelineStateInfo]` | All DLT pipelines |
| `get` | `pipeline_id: str` | `GetPipelineResponse` | Pipeline details + state |
| `list_updates` | `pipeline_id, max_results?, page_token?, until_update_id?` | `ListUpdatesResponse` | **Pipeline run history** |
| `get_update` | `pipeline_id, update_id` | `GetUpdateResponse` | Specific pipeline run details |
| `list_pipeline_events` | `pipeline_id, filter?, max_results?, order_by?, page_token?` | `Iterator[PipelineEvent]` | **Pipeline events** (filterable by log level, timestamp) |
| `start_update` | `pipeline_id + config` | `StartUpdateResponse` | Trigger pipeline |
| `stop` | `pipeline_id` | `None` | Stop pipeline |
| `delete` | `pipeline_id` | `None` | Delete pipeline |
| `create` | config | `CreatePipelineResponse` | Create pipeline |
| `update` | `pipeline_id + config` | `None` | Update pipeline |

### 1.9 Instance Pools API

**Access:** `w.instance_pools.<method>()`
**REST Base:** `/api/2.0/instance-pools/`

| Method | Parameters | Return Type | Description |
|--------|-----------|-------------|-------------|
| `list` | — | `Iterator[InstancePoolAndStats]` | **All pools with statistics** (idle/used/pending counts) |
| `get` | `instance_pool_id: str` | `GetInstancePool` | Pool details + stats |
| `create` | `instance_pool_name, node_type_id + config` | `CreateInstancePoolResponse` | Create pool |
| `edit` | `instance_pool_id + config` | `None` | Modify pool |
| `delete` | `instance_pool_id: str` | `None` | Delete pool |
| `get_permissions` | `instance_pool_id` | `InstancePoolPermissions` | Pool ACLs |
| `get_permission_levels` | `instance_pool_id` | response | Permission levels |
| `set_permissions` | `instance_pool_id, acl` | `InstancePoolPermissions` | Replace permissions |
| `update_permissions` | `instance_pool_id, acl` | `InstancePoolPermissions` | Modify permissions |

#### InstancePoolAndStats Key Fields

| Field | Type | Description |
|-------|------|-------------|
| `stats.used_count` | `int` | Instances currently in use |
| `stats.idle_count` | `int` | Idle instances ready |
| `stats.pending_used_count` | `int` | Pending allocation for use |
| `stats.pending_idle_count` | `int` | Pending allocation for pool |
| `min_idle_instances` | `int` | Target minimum idle |
| `max_capacity` | `int` | Maximum pool size |

### 1.10 Cluster Policies API

**Access:** `w.cluster_policies.<method>()`
**REST Base:** `/api/2.0/policies/clusters/`

| Method | Parameters | Return Type | Description |
|--------|-----------|-------------|-------------|
| `list` | `sort_column?, sort_order?` | `Iterator[Policy]` | All accessible policies |
| `get` | `policy_id: str` | `Policy` | Policy definition + constraints |
| `create` | `definition, name + config` | `CreatePolicyResponse` | Create policy |
| `edit` | `policy_id + config` | `None` | Update policy |
| `delete` | `policy_id: str` | `None` | Delete policy |
| `get_permissions` | `cluster_policy_id` | `ClusterPolicyPermissions` | Policy ACLs |
| `get_permission_levels` | `cluster_policy_id` | response | Permission levels |
| `set_permissions` | `cluster_policy_id, acl` | `ClusterPolicyPermissions` | Replace permissions |
| `update_permissions` | `cluster_policy_id, acl` | `ClusterPolicyPermissions` | Modify permissions |

### 1.11 Serving Endpoints API

**Access:** `w.serving_endpoints.<method>()`
**REST Base:** `/api/2.0/serving-endpoints/`

| Method | Parameters | Return Type | Description |
|--------|-----------|-------------|-------------|
| `list` | — | `Iterator[ServingEndpointDetailed]` | All serving endpoints |
| `get` | `name: str` | `ServingEndpointDetailed` | Endpoint details + state |
| `export_metrics` | `name: str` | `ExportMetricsResponse` | **Prometheus/OpenMetrics format metrics** |
| `build_logs` | `name, served_model_name` | `BuildLogsResponse` | Model build logs |
| `logs` | `name, served_model_name` | `ServerLogsResponse` | Model service logs |
| `get_open_api` | `name: str` | `GetOpenApiResponse` | Query schema |
| `create` | `name + config` | `Wait[ServingEndpointDetailed]` | Create endpoint |
| `update_config` | `name + config` | `Wait[ServingEndpointDetailed]` | Update endpoint |
| `delete` | `name: str` | `None` | Delete endpoint |

### 1.12 MLflow Experiments API

**Access:** `w.experiments.<method>()`
**REST Base:** `/api/2.0/mlflow/`

| Method | Parameters | Return Type | Description |
|--------|-----------|-------------|-------------|
| `list_experiments` | `max_results?, page_token?, view_type?` | `Iterator[Experiment]` | All experiments |
| `get_experiment` | `experiment_id: str` | `GetExperimentResponse` | Experiment metadata |
| `search_runs` | `experiment_ids?, filter?, max_results?, order_by?, page_token?, run_view_type?` | `Iterator[Run]` | **Search runs by metrics/params/tags** |
| `get_run` | `run_id, run_uuid?` | `GetRunResponse` | **Run metadata + metrics + params + tags** |
| `list_artifacts` | `page_token?, path?, run_id?, run_uuid?` | `Iterator[FileInfo]` | Run artifacts |
| `get_history` | `metric_key, max_results?, page_token?, run_id?, run_uuid?` | `Iterator[Metric]` | **Metric time series for a run** |

### 1.13 Token Management API

**Access:** `w.token_management.<method>()`
**REST Base:** `/api/2.0/token-management/`

| Method | Parameters | Return Type | Description |
|--------|-----------|-------------|-------------|
| `list` | `created_by_id?, created_by_username?` | `Iterator[TokenInfo]` | All workspace tokens |
| `get` | `token_id: str` | `GetTokenResponse` | Token details |
| `create_obo_token` | `application_id, comment?, lifetime_seconds?` | `CreateOboTokenResponse` | Service principal token |
| `delete` | `token_id: str` | `None` | Revoke token |

### 1.14 Global Init Scripts API

**Access:** `w.global_init_scripts.<method>()`
**REST Base:** `/api/2.0/global-init-scripts/`

| Method | Parameters | Return Type | Description |
|--------|-----------|-------------|-------------|
| `list` | — | `Iterator[GlobalInitScriptDetails]` | All init scripts |
| `get` | `script_id: str` | `GlobalInitScriptDetailsWithContent` | Script content + metadata |
| `create` | `name, script, enabled?, position?` | `CreateResponse` | Create script |
| `update` | `script_id, name, script, enabled?, position?` | `None` | Update script |
| `delete` | `script_id: str` | `None` | Delete script |

---

## 2. Spark REST API v1 Endpoints

**Base URL (local Spark):** `http://localhost:4040/api/v1`
**Base URL (Databricks proxy):** `https://<workspace>/driver-proxy-api/o/<org-id>/<cluster-id>/<port>/api/v1`
**Base URL (History Server):** `http://<server>:18080/api/v1`

### 2.1 Applications

| Endpoint | Method | Parameters | Description |
|----------|--------|-----------|-------------|
| `/applications` | GET | `status=[completed\|running]`, `minDate`, `maxDate`, `minEndDate`, `maxEndDate`, `limit` | **List all Spark applications** |
| `/applications/[app-id]` | GET | — | Single application metadata |

**Response fields:** `id`, `name`, `attempts[].startTime`, `attempts[].endTime`, `attempts[].sparkUser`, `attempts[].completed`, `attempts[].duration`

### 2.2 Jobs

| Endpoint | Method | Parameters | Description |
|----------|--------|-----------|-------------|
| `/applications/[app-id]/jobs` | GET | `status=[running\|succeeded\|failed\|unknown]` | **All Spark jobs** |
| `/applications/[app-id]/jobs/[job-id]` | GET | — | Single job details |

**Response fields:** `jobId`, `name`, `description`, `submissionTime`, `completionTime`, `stageIds[]`, `jobGroup`, `status` (RUNNING, SUCCEEDED, FAILED, UNKNOWN), `numTasks`, `numActiveTasks`, `numCompletedTasks`, `numSkippedTasks`, `numFailedTasks`, `numKilledTasks`, `numActiveStages`, `numCompletedStages`, `numSkippedStages`, `numFailedStages`

### 2.3 Stages

| Endpoint | Method | Parameters | Description |
|----------|--------|-----------|-------------|
| `/applications/[app-id]/stages` | GET | `status=[active\|complete\|pending\|failed]`, `details=true`, `taskStatus`, `withSummaries=true`, `quantiles` | **All stages** |
| `/applications/[app-id]/stages/[stage-id]` | GET | `details`, `taskStatus`, `withSummaries`, `quantiles` | All attempts for stage |
| `/applications/[app-id]/stages/[stage-id]/[attempt-id]` | GET | `details`, `taskStatus`, `withSummaries`, `quantiles` | **Specific stage attempt** |
| `/applications/[app-id]/stages/[stage-id]/[attempt-id]/taskSummary` | GET | `quantiles` (e.g., `0.01,0.5,0.99`) | **Task metrics distribution** |
| `/applications/[app-id]/stages/[stage-id]/[attempt-id]/taskList` | GET | `offset`, `length`, `sortBy=[runtime\|-runtime]`, `status` | **Paginated task list** |

**Stage response fields:** `stageId`, `attemptId`, `status`, `name`, `numTasks`, `numActiveTasks`, `numCompleteTasks`, `numFailedTasks`, `numKilledTasks`, `executorRunTime`, `executorCpuTime`, `submissionTime`, `completionTime`, `inputBytes`, `inputRecords`, `outputBytes`, `outputRecords`, `shuffleReadBytes`, `shuffleReadRecords`, `shuffleWriteBytes`, `shuffleWriteRecords`, `memoryBytesSpilled`, `diskBytesSpilled`, `schedulingPool`, `accumulatorUpdates[]`

**Task summary fields (at each quantile):** `executorRunTime`, `executorCpuTime`, `executorDeserializeTime`, `executorDeserializeCpuTime`, `resultSerializationTime`, `jvmGcTime`, `shuffleFetchWaitTime`, `shuffleWriteTime`, `gettingResultTime`, `schedulerDelay`, `peakExecutionMemory`, `memoryBytesSpilled`, `diskBytesSpilled`, `inputBytes`, `inputRecords`, `outputBytes`, `outputRecords`, `shuffleReadBytes`, `shuffleReadRecords`, `shuffleRemoteReadBytes`, `shuffleWriteBytes`, `shuffleWriteRecords`, `resultSize`

### 2.4 Executors

| Endpoint | Method | Parameters | Description |
|----------|--------|-----------|-------------|
| `/applications/[app-id]/executors` | GET | — | **Active executors only** |
| `/applications/[app-id]/allexecutors` | GET | — | **All executors (active + dead)** |
| `/applications/[app-id]/executors/[executor-id]/threads` | GET | — | **Thread dump** (live app only, NOT on history server) |

**Executor response fields:**

| Field | Type | Description |
|-------|------|-------------|
| `id` | `str` | Executor ID ("driver" or numeric) |
| `hostPort` | `str` | host:port |
| `isActive` | `bool` | Currently active |
| `rddBlocks` | `int` | Cached RDD blocks |
| `memoryUsed` | `long` | Storage memory used (bytes) |
| `diskUsed` | `long` | Disk space for RDD storage |
| `totalCores` | `int` | Available CPU cores |
| `maxTasks` | `int` | Max concurrent tasks |
| `activeTasks` | `int` | **Currently running tasks** |
| `failedTasks` | `int` | Total failed tasks |
| `completedTasks` | `int` | Total completed tasks |
| `totalTasks` | `int` | Total tasks ever |
| `totalDuration` | `long` | Total JVM execution time (ms) |
| `totalGCTime` | `long` | **Total GC time (ms)** |
| `totalInputBytes` | `long` | Total input bytes read |
| `totalShuffleRead` | `long` | Total shuffle bytes read |
| `totalShuffleWrite` | `long` | Total shuffle bytes written |
| `isBlacklisted` | `bool` | Blacklisted/excluded |
| `maxMemory` | `long` | **Total available storage memory** |
| `addTime` | `str` | When executor was added |
| `removeTime` | `str` | When removed (if dead) |
| `removeReason` | `str` | Why removed |
| `memoryMetrics` | `object` | `usedOnHeapStorageMemory`, `usedOffHeapStorageMemory`, `totalOnHeapStorageMemory`, `totalOffHeapStorageMemory` |
| `peakMemoryMetrics` | `object` | Peak values for: `JVMHeapMemory`, `JVMOffHeapMemory`, `OnHeapExecutionMemory`, `OffHeapExecutionMemory`, `OnHeapStorageMemory`, `OffHeapStorageMemory`, `OnHeapUnifiedMemory`, `OffHeapUnifiedMemory`, `DirectPoolMemory`, `MappedPoolMemory`, `ProcessTreeJVMVMemory`, `ProcessTreeJVMRSSMemory`, `ProcessTreePythonVMemory`, `ProcessTreePythonRSSMemory`, `ProcessTreeOtherVMemory`, `ProcessTreeOtherRSSMemory`, `MinorGCCount`, `MinorGCTime`, `MajorGCCount`, `MajorGCTime`, `TotalGCTime` |

### 2.5 Storage / RDD

| Endpoint | Method | Parameters | Description |
|----------|--------|-----------|-------------|
| `/applications/[app-id]/storage/rdd` | GET | — | All cached RDDs |
| `/applications/[app-id]/storage/rdd/[rdd-id]` | GET | — | Specific RDD storage details |

**Response fields:** `id`, `name`, `numPartitions`, `numCachedPartitions`, `storageLevel`, `memoryUsed`, `diskUsed`, `dataDistribution[]` (per-executor breakdown), `partitions[]`

### 2.6 Environment

| Endpoint | Method | Parameters | Description |
|----------|--------|-----------|-------------|
| `/applications/[app-id]/environment` | GET | — | **Full application environment** |

**Response sections:** `runtime` (javaVersion, scalaVersion, sparkVersion), `sparkProperties[]`, `hadoopProperties[]`, `systemProperties[]`, `metricsProperties[]`, `classpathEntries[]`, `resourceProfiles[]`

### 2.7 SQL

| Endpoint | Method | Parameters | Description |
|----------|--------|-----------|-------------|
| `/applications/[app-id]/sql` | GET | `details=[true\|false]`, `planDescription=[true\|false]`, `offset`, `length` | **All SQL queries** |
| `/applications/[app-id]/sql/[execution-id]` | GET | `details=[true\|false]`, `planDescription=[true\|false]` | **Single SQL query details** |

**Response fields:** `id`, `status` (RUNNING, COMPLETED, FAILED), `description`, `planDescription`, `submissionTime`, `duration`, `runningJobIds[]`, `successJobIds[]`, `failedJobIds[]`, `nodes[]` (execution plan nodes with metrics)

**Node metrics include:** `number of output rows`, `scan time total`, `duration total`, `number of files read`, `size of files read`, `number of partitions read`, `shuffle bytes written`, `shuffle records written`, `shuffle bytes read`, `shuffle records read`, `spill size`, `peak memory total`

### 2.8 Streaming

| Endpoint | Method | Parameters | Description |
|----------|--------|-----------|-------------|
| `/applications/[app-id]/streaming/statistics` | GET | — | **Streaming statistics** |
| `/applications/[app-id]/streaming/receivers` | GET | — | All streaming receivers |
| `/applications/[app-id]/streaming/receivers/[stream-id]` | GET | — | Specific receiver details |
| `/applications/[app-id]/streaming/batches` | GET | — | All retained batches |
| `/applications/[app-id]/streaming/batches/[batch-id]` | GET | — | Batch details |
| `/applications/[app-id]/streaming/batches/[batch-id]/operations` | GET | — | Batch output operations |
| `/applications/[app-id]/streaming/batches/[batch-id]/operations/[op-id]` | GET | — | Specific operation details |

**Note:** These are for Spark Streaming (DStreams). Structured Streaming metrics require `spark.sql.streaming.metricsEnabled=true` and appear in the metrics system, not via these endpoints.

### 2.9 Event Logs

| Endpoint | Method | Parameters | Description |
|----------|--------|-----------|-------------|
| `/applications/[base-app-id]/logs` | GET | — | Download all event logs (zip) |
| `/applications/[base-app-id]/[attempt-id]/logs` | GET | — | Download specific attempt logs (zip) |

### 2.10 Metrics / Prometheus

| Endpoint | Port | Description |
|----------|------|-------------|
| `/metrics/json/` | 4040 (driver) | All driver metrics as JSON |
| `/metrics/prometheus/` | 4040 (driver) | All driver metrics in Prometheus format |
| `/metrics/executors/prometheus/` | 4040 (driver) | **Executor metrics in Prometheus format** |
| `/metrics/master/json/` | 8080 (master) | Master metrics JSON (standalone mode) |
| `/metrics/master/prometheus/` | 8080 (master) | Master metrics Prometheus |
| `/metrics/applications/json/` | 8080 (master) | Application metrics JSON |
| `/metrics/applications/prometheus/` | 8080 (master) | Application metrics Prometheus |

**Note:** Prometheus servlet is **experimental** in Spark 4.x. Must be enabled via:
```
spark.metrics.conf.*.sink.prometheus.class=org.apache.spark.metrics.sink.PrometheusServlet
```

### 2.11 Version

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/version` | GET | Returns Spark version string |

---

## 3. Spark Metrics System

The Spark metrics system is based on the **Dropwizard Metrics Library** and provides deep instrumentation beyond the REST API.

### Metric Sources

| Source | Component | Key Metrics |
|--------|-----------|-------------|
| **JvmSource** | driver/executor | heap/non-heap memory, GC counts/time, thread counts, buffer pool |
| **ExecutorSource** | executor | bytesRead/Written, shuffleRead/Write, cpuTime, runTime, GC time |
| **DAGSchedulerSource** | driver | stage counts (waiting/running/failed), job counts (active/all) |
| **BlockManagerSource** | driver/executor | memory used/remaining, disk space |
| **ShuffleMetrics** | executor | shuffle read/write bytes, fetch wait time, local/remote blocks |
| **CodeGenerator** | driver | compilationTime, sourceCodeSize, generatedClassSize, generatedMethodSize |
| **HiveExternalCatalog** | driver | fileCacheHits, filesDiscovered, hiveClientCalls, parallelListingJobCount, partitionsFetched |
| **LiveListenerBus** | driver | events posted/processed/dropped, queue size, processing time |
| **StructuredStreaming** | driver | inputRate, processingRate, latency, stateRowsTotal (if enabled) |
| **AccumulatorSource** | custom | User-defined accumulators exposed as metrics |

### Available Sinks

| Sink | Format | Use Case |
|------|--------|----------|
| `MetricsServlet` | JSON via HTTP | Built-in, always available at `/metrics/json` |
| `PrometheusServlet` | Prometheus text | Time-series monitoring (experimental) |
| `JmxSink` | JMX MBeans | JMX monitoring tools |
| `GraphiteSink` | Graphite protocol | Graphite/Carbon backends |
| `StatsdSink` | StatsD protocol | StatsD/Datadog backends |
| `Slf4jSink` | Log messages | Log aggregation systems |
| `CSVSink` | CSV files | Local file analysis |
| `ConsoleSink` | stdout | Debugging |
| `GangliaSink` | Ganglia protocol | Ganglia monitoring (requires custom build) |

### Configuration

```properties
# In spark.metrics.conf or via spark.metrics.conf.* Spark config
*.sink.servlet.class=org.apache.spark.metrics.sink.MetricsServlet
*.sink.servlet.path=/metrics/json

# Enable JVM metrics
*.source.jvm.class=org.apache.spark.metrics.source.JvmSource

# Executor metrics polling interval (ms)
spark.executor.metrics.pollingInterval=1000

# Structured streaming metrics
spark.sql.streaming.metricsEnabled=true

# Custom namespace (useful for cross-run tracking)
spark.metrics.namespace=${spark.app.name}
```

---

## 4. Authentication & Configuration

### Databricks SDK Authentication Flow

The SDK (`WorkspaceClient`) resolves credentials in this order:

1. **Hard-coded arguments** in `WorkspaceClient(host=..., token=...)` (not recommended)
2. **Environment variables**: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, etc.
3. **Config file `~/.databrickscfg`** — `DEFAULT` profile (or profile specified by `DATABRICKS_CONFIG_PROFILE`)
4. **Azure CLI** credentials (Azure-only fallback)

### Config File Format (`~/.databrickscfg`)

```ini
[DEFAULT]
host = https://adb-1234567890.12.azuredatabricks.net
token = dapi1234567890abcdef

[yad2-prod]
host = https://adb-1234567890.12.azuredatabricks.net
token = dapi1234567890abcdef

[yad2-dev]
host = https://adb-0987654321.12.azuredatabricks.net
token = dapi0987654321fedcba
```

### Using a Named Profile

```python
from databricks.sdk import WorkspaceClient

# Explicit profile
w = WorkspaceClient(profile="yad2-prod")

# Or via environment variable
# export DATABRICKS_CONFIG_PROFILE=yad2-prod
w = WorkspaceClient()
```

### Authentication Methods Supported

| Method | Config Keys | Description |
|--------|-------------|-------------|
| **PAT (Personal Access Token)** | `host`, `token` | Most common for CLI/SDK |
| **OAuth M2M (Client Credentials)** | `host`, `client_id`, `client_secret` | Service principal |
| **OAuth U2M (Authorization Code)** | `host` | Browser-based OAuth flow |
| **Basic Auth** | `host`, `username`, `password` | AWS only, deprecated |
| **Azure CLI** | auto-detected | Falls back to `az login` session |
| **Azure Service Principal** | `ARM_CLIENT_ID`, `ARM_CLIENT_SECRET`, `ARM_TENANT_ID` | Azure AD app registration |

### Raw HTTP API Access

```python
# Use the SDK's api_client for any REST endpoint not covered by typed methods
w = WorkspaceClient(profile="yad2-prod")

# Direct REST call
response = w.api_client.do(
    method="GET",
    path="/api/2.0/clusters/get",
    body={"cluster_id": "0311-160038-vjuv0ah9"}
)
```

---

## 5. Spark Application ID Discovery

The Spark application ID is needed for all Spark REST API v1 calls. Here are the methods to discover it:

### Method 1: From ClusterDetails (SDK)

```python
w = WorkspaceClient(profile="yad2-prod")
cluster = w.clusters.get("0311-160038-vjuv0ah9")
spark_context_id = cluster.spark_context_id  # Changes on driver restart
```

**Note:** `spark_context_id` is NOT the same as the Spark application ID. The application ID format is typically `app-YYYYMMDDHHMMSS-NNNN` or `application_NNNNNNNNNNNN_NNNN` (YARN).

### Method 2: From Spark REST API

```python
import requests

base = "https://<workspace>/driver-proxy-api/o/<org-id>/<cluster-id>/40001"
resp = requests.get(f"{base}/api/v1/applications", headers={"Authorization": "Bearer <token>"})
apps = resp.json()
app_id = apps[0]["id"]  # Most recent application
```

### Method 3: From Command Execution

```python
# Execute on cluster to get app ID
w.command_execution.execute_and_wait(
    cluster_id="0311-160038-vjuv0ah9",
    language="python",
    command="print(spark.sparkContext.applicationId)"
)
```

### Method 4: From Environment Endpoint

```python
# If you can access the REST API, /environment reveals the app ID
resp = requests.get(f"{base}/api/v1/applications/{app_id}/environment")
# spark.app.id in sparkProperties
```

---

## 6. Databricks Driver Proxy API

The Databricks driver proxy allows external access to web services running on cluster driver nodes.

### URL Pattern

```
https://<workspace-url>/driver-proxy-api/o/<org-id>/<cluster-id>/<port>/<path>
```

**Components:**
- `<workspace-url>`: Your Databricks workspace URL (e.g., `adb-1234567890.12.azuredatabricks.net`)
- `<org-id>`: Organization/workspace ID (the numeric part after `o/` in workspace URL, or `0` for some deployments)
- `<cluster-id>`: Target cluster ID (e.g., `0311-160038-vjuv0ah9`)
- `<port>`: Port on the driver node to proxy to
- `<path>`: The URL path forwarded to the driver service

### Known Ports

| Port | Service | Description |
|------|---------|-------------|
| **40001** | Spark UI | Spark web UI (standard Databricks proxy port) |
| **4040** | Spark UI (direct) | Standard Spark UI port (may not be proxied) |
| **7681** | Web Terminal | Driver web terminal (restricted to terminal only) |
| **Various** | Custom services | User-started web services on driver |

### Authentication

All proxy requests require the same authentication as Databricks API calls:

```python
headers = {
    "Authorization": "Bearer <personal-access-token>"
}
```

### Example: Accessing Spark REST API via Proxy

```python
import requests

WORKSPACE = "https://adb-1234567890.12.azuredatabricks.net"
ORG_ID = "1234567890"  # or "0" for some deployments
CLUSTER_ID = "0311-160038-vjuv0ah9"
PORT = 40001  # Spark UI proxy port
TOKEN = "<your-pat>"

base = f"{WORKSPACE}/driver-proxy-api/o/{ORG_ID}/{CLUSTER_ID}/{PORT}"
headers = {"Authorization": f"Bearer {TOKEN}"}

# Discover application ID
apps = requests.get(f"{base}/api/v1/applications", headers=headers).json()
app_id = apps[0]["id"]

# Get executors
executors = requests.get(
    f"{base}/api/v1/applications/{app_id}/executors",
    headers=headers
).json()

# Get active jobs
jobs = requests.get(
    f"{base}/api/v1/applications/{app_id}/jobs?status=running",
    headers=headers
).json()
```

### Port Discovery

The Spark UI port on Databricks is not always 40001. To discover it:

1. **Check ClusterDetails:** `cluster.jdbc_port` gives the JDBC port; the Spark UI is typically on a nearby port
2. **Try common ports:** 40001, 40000, 4040
3. **Use Command Execution:** Run `spark.sparkContext.uiWebUrl` on the cluster
4. **Check Spark config:** `spark.ui.port` in `cluster.spark_conf`

### Limitations

- The web terminal proxy on port 7681 is restricted to terminal use only
- Custom web services require the cluster's security mode to allow it
- Proxy adds latency vs. direct driver access
- Large responses may be truncated or slow through the proxy

---

## 7. Rate Limits & Performance Considerations

### Databricks REST API

| Aspect | Detail |
|--------|--------|
| **Rate limits** | Databricks applies per-workspace rate limits (typically 30-60 req/sec for most endpoints). Exact limits vary by plan and endpoint. |
| **Cluster.get polling** | Safe at 5-10 second intervals. The endpoint is lightweight. |
| **Jobs.list_runs** | Supports pagination; avoid fetching all runs at once for high-volume workspaces. |
| **Events API** | Returns up to 500 events per page. Use pagination for full history. |
| **SDK retry logic** | The SDK has built-in retry with exponential backoff for 429 (rate limit) and 503 (service unavailable) responses. |

### Spark REST API

| Aspect | Detail |
|--------|--------|
| **No documented rate limits** | The Spark REST API has no explicit rate limits, but is bounded by driver memory and CPU. |
| **Data retention** | Governed by `spark.ui.retainedJobs` (default 1000) and `spark.ui.retainedStages` (default 1000). Older data is garbage collected. |
| **`/stages?details=true`** | **EXPENSIVE** - Returns all task-level data. Can be very large for stages with millions of tasks. Use pagination. |
| **`/executors`** | Lightweight; safe for frequent polling (every 1-2 seconds). |
| **`/sql`** | Can return large plan descriptions. Use `planDescription=false` if not needed. |
| **Proxy overhead** | Databricks driver proxy adds ~50-200ms latency per request compared to direct driver access. |
| **History server lag** | If using history server, updates are delayed by `spark.history.fs.update.interval` (default 10 seconds). |

### Recommended Polling Intervals for dbxtop

| Data Type | Recommended Interval | Reason |
|-----------|---------------------|--------|
| Cluster state (SDK) | 5-10 seconds | Lightweight; state changes are infrequent |
| Executor metrics (Spark) | 2-5 seconds | Core dashboard metric; relatively lightweight |
| Active jobs (Spark) | 2-5 seconds | Quick overview of running work |
| Stage progress (Spark) | 3-5 seconds | Task counts change frequently during execution |
| SQL queries (Spark) | 5-10 seconds | Moderate overhead |
| Job runs (SDK) | 10-30 seconds | Infrequent changes |
| Library status (SDK) | 30-60 seconds | Rarely changes |
| Cluster events (SDK) | 15-30 seconds | Append-only, check for new events |
| Instance pool stats (SDK) | 30 seconds | Slow-changing |

---

## 8. Gaps: What's in the UI but NOT in the API

### Not Available via REST API

| Feature | Available In | Not Available Via | Notes |
|---------|-------------|-------------------|-------|
| **DAG visualization** | Spark UI | REST API | No graph/DAG structure data exposed |
| **Executor stdout/stderr logs** | Spark UI | REST API | Log files are on executor nodes; not proxied |
| **Real-time log streaming** | Spark UI | REST API | Only batch download via `/logs` |
| **Live thread dumps (history server)** | Live Spark UI | History Server REST | `/executors/[id]/threads` is live-only |
| **Performance timeline graphs** | Spark UI | REST API | Visual-only; raw data is available |
| **Stage-level DAG with shuffle dependencies** | Spark UI | REST API | Stage lineage not in API |
| **SQL query plan visualization** | Spark UI | REST API | `planDescription` is text, not graph |
| **Notebook cell output** | Databricks UI | REST API (partial) | `get_run_output` only for completed notebook runs, max 5MB |
| **Ganglia-style cluster metrics** | Databricks UI (Metrics tab) | REST API | Databricks-specific cluster metrics dashboard not exposed |
| **Databricks cluster Metrics tab** | Databricks UI | Any API | CPU, memory, network, disk I/O charts from the Metrics tab are NOT available via API |
| **Driver/executor Spark logs** | Databricks UI (Log tab) | REST API | Cluster log delivery to DBFS/S3 is the workaround |
| **Structured Streaming progress** | StreamingQueryListener | REST API (partially) | Requires in-process listener or metrics sink; not in REST API v1 |

### Partially Available

| Feature | What's Available | What's Missing |
|---------|-----------------|----------------|
| **SQL execution plans** | Text plan via `planDescription` | Visual plan graph, cost estimates |
| **Task-level metrics** | Via `/stages/.../taskList` with pagination | Aggregation must be done client-side |
| **Memory breakdown** | `memoryMetrics` + `peakMemoryMetrics` on executors | Real-time GC pressure, heap histogram |
| **Shuffle details** | Aggregate bytes/records | Per-partition skew analysis |
| **Cluster utilization** | Executor count + cores + memory (static) | Real-time CPU/memory utilization percentages |

---

## 9. Shared vs Single-User Cluster Availability

| API / Feature | Single-User | Shared Access Mode | Notes |
|--------------|-------------|-------------------|-------|
| **Databricks SDK (clusters.get, events, etc.)** | Full access | Full access | No difference |
| **Spark REST API via proxy** | Full access | Full access | REST API is read-only, unaffected by access mode |
| **sparkContext direct access** | Available | **BLOCKED** | `sc._jvm`, `sparkContext` blocked on shared clusters |
| **Command Execution API** | Full access | Limited | Commands run in isolated context; some Spark internals blocked |
| **SparkAPI (Splink)** | Works directly | **Requires workaround** | `os.environ.pop("DATABRICKS_RUNTIME_VERSION")` before init |
| **Executor thread dumps** | Available | May be restricted | Depends on cluster security configuration |
| **Library installation** | Full access | Restricted | May require admin on shared clusters |
| **Custom web services on driver** | Allowed | **Restricted** | Security mode may block custom ports |

### Impact on dbxtop

The primary data sources for dbxtop (Databricks SDK + Spark REST API) work on both cluster modes. The key constraint is:

- **Cannot use `sparkContext` programmatically on shared clusters** -- must rely on REST API for Spark metrics
- **The REST API proxy should work regardless** -- it's a read-only HTTP interface to the driver's web UI server

---

## 10. Recommendations for dbxtop

### Architecture

```
dbxtop
├── Data Layer
│   ├── DatabricksClient (SDK wrapper)
│   │   ├── clusters.get() → cluster state, nodes, memory
│   │   ├── clusters.events() → lifecycle events
│   │   ├── jobs.list_runs() → active/recent runs
│   │   └── libraries.cluster_status() → library health
│   ├── SparkRESTClient (HTTP via driver proxy)
│   │   ├── /applications → app ID discovery
│   │   ├── /executors → executor health, GC, tasks
│   │   ├── /jobs → Spark job progress
│   │   ├── /stages → stage progress, task distribution
│   │   └── /sql → SQL query tracking
│   └── MetricsPoller (async polling loop)
│       ├── Fast loop (2-5s): executors, jobs, stages
│       ├── Medium loop (10-15s): cluster state, SQL
│       └── Slow loop (30-60s): events, libraries, pools
├── Display Layer
│   ├── Cluster Overview Panel
│   ├── Executor Health Panel
│   ├── Job/Stage Progress Panel
│   ├── SQL Query Panel
│   └── Events/Alerts Panel
└── Config
    ├── ~/.databrickscfg profile resolution
    ├── Cluster ID (required)
    └── Refresh intervals (configurable)
```

### Priority Endpoints (Must Have)

1. **`clusters.get(cluster_id)`** — Cluster state, memory, cores, nodes
2. **`/api/v1/applications`** — App ID discovery
3. **`/api/v1/applications/{id}/executors`** — Executor health, GC, task counts
4. **`/api/v1/applications/{id}/jobs`** — Spark job progress
5. **`/api/v1/applications/{id}/stages`** — Stage progress, task distribution
6. **`clusters.events(cluster_id)`** — Cluster lifecycle events
7. **`jobs.list_runs(active_only=True)`** — Active Databricks job runs

### Secondary Endpoints (Nice to Have)

8. **`/api/v1/applications/{id}/sql`** — SQL query tracking
9. **`/api/v1/applications/{id}/storage/rdd`** — Cached data monitoring
10. **`/api/v1/applications/{id}/environment`** — Configuration display
11. **`libraries.cluster_status(cluster_id)`** — Library health
12. **`instance_pools.list()`** — Pool utilization
13. **`warehouses.list()`** — SQL warehouse status
14. **`pipelines.list_pipelines()`** — DLT pipeline status

### Key Implementation Notes

1. **Error handling for terminated clusters:** `clusters.get()` returns TERMINATED state; Spark REST API returns connection errors. Detect TERMINATED state first, skip Spark API calls.

2. **Application ID caching:** The app ID changes on cluster restart. Cache it but re-fetch if Spark API returns 404.

3. **Graceful degradation:** If Spark REST API proxy is unreachable (wrong port, cluster restarting), fall back to SDK-only mode showing cluster-level metrics.

4. **The `/allexecutors` endpoint is better than `/executors`** for tracking executor churn (added/removed over time).

5. **Stage task summary** (`/stages/{id}/{attempt}/taskSummary?quantiles=0.0,0.25,0.5,0.75,1.0`) gives data-skew insight without fetching all tasks.

6. **SDK `api_client.do()`** provides raw HTTP for any endpoint not covered by typed SDK methods, including custom driver proxy paths.

7. **Cluster events with `event_types` filter:** Use `["AUTOSCALING_STATS_REPORT"]` for autoscaling telemetry without noise from other events.

8. **Structured Streaming:** Not well-served by REST API. Consider using Command Execution API to run `spark.streams.active` for streaming query status on non-shared clusters.

---

## Appendix: Complete Endpoint Count Summary

### Databricks SDK Methods (Monitoring-Relevant)

| Service | Method Count | Key Monitoring Methods |
|---------|-------------|----------------------|
| Clusters | 30 | get, list, events, resize, start, restart |
| Jobs | 15 | list, get, list_runs, get_run, get_run_output |
| Command Execution | 12 | execute, command_status, context_status |
| Libraries | 4 | all_cluster_statuses, cluster_status |
| SQL Warehouses | 14 | list, get, start, stop |
| SQL Statement Execution | 4 | execute_statement, get_statement |
| SQL Query History | 1 | list |
| Pipelines (DLT) | 10 | list_pipelines, get, list_updates, list_pipeline_events |
| Instance Pools | 9 | list, get |
| Cluster Policies | 9 | list, get |
| Serving Endpoints | 8 | list, get, export_metrics, build_logs, logs |
| MLflow Experiments | 6 | list_experiments, search_runs, get_run, get_history |
| Token Management | 4 | list, get |
| Global Init Scripts | 5 | list, get |
| **Total** | **131** | |

### Spark REST API v1 Endpoints

| Category | Endpoint Count | Key Endpoints |
|----------|---------------|---------------|
| Applications | 2 | /applications, /applications/{id} |
| Jobs | 2 | /jobs, /jobs/{id} |
| Stages | 5 | /stages, /stages/{id}, /{id}/{attempt}, /taskSummary, /taskList |
| Executors | 3 | /executors, /allexecutors, /threads |
| Storage | 2 | /storage/rdd, /storage/rdd/{id} |
| Environment | 1 | /environment |
| SQL | 2 | /sql, /sql/{id} |
| Streaming | 7 | /statistics, /receivers, /batches + sub-paths |
| Event Logs | 2 | /logs, /{attempt}/logs |
| Metrics | 6 | /metrics/json, /metrics/prometheus, /metrics/executors/prometheus, etc. |
| Version | 1 | /version |
| **Total** | **33** | |

### Grand Total: ~164 API endpoints relevant to cluster monitoring
