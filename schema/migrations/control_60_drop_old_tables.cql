-- Dropping old tables from older versions of the code


USE @@KEYSPACE@@;

-- Drop tables from before all scaling group information was consolidated into
-- one table

-- from old version of control_15_create_scaling_groups.cql
DROP TABLE scaling_config;

-- from control_20_create_launch_configs.cql
DROP TABLE launch_config;

-- from control_50_create_group_state.cql
DROP TABLE group_state;



-- from control_51_create_lock_table.cql, because no longer use DB-based locks
DROP TABLE locks;



-- from control_60_create_audit_log.cql, because audit logs go elsewhere
DROP TABLE audit_log;
