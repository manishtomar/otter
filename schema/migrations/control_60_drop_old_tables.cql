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
