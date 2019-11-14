CREATE SCHEMA "extension'test";

-- use  a schema name with escape character
SET search_path TO "extension'test";

-- create an extension on the given search_path
-- the extension is on contrib, so should be avaliable for the regression tests
CREATE EXTENSION hstore;

--  make sure that both the schema and the extension is distributed
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname = 'hstore');
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_namespace WHERE nspname = 'extension''test');

CREATE TABLE test_table (key int, value hstore);
SELECT create_distributed_table('test_table', 'key');

--  make sure that the table is also distributed object now
SELECT count(*) from pg_dist_partition where logicalrelid='extension''test.test_table'::regclass;

CREATE TYPE two_hstores AS (hstore_1 hstore, hstore_2 hstore);

-- verify that the type that depends on the extension is also marked as distributed
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_type WHERE typname = 'two_hstores' AND typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'extension''test'));

-- SET client_min_messages TO WARNING before executing a DROP EXTENSION statement
SET client_min_messages TO WARNING;
-- now drop hstore for following tests
DROP EXTENSION hstore CASCADE;
-- restore client_min_messages after DROP EXTENSION
RESET client_min_messages;

-- now CREATE EXTENSION within a transction block and with version defined and commit it
BEGIN;
	CREATE EXTENSION isn WITH VERSION '1.1' SCHEMA public;

	-- now, create a reference table relying on the data types
	CREATE TABLE ref_table (a public.issn);
	SELECT create_reference_table('ref_table');

	-- now, update to a newer version
	ALTER EXTENSION isn UPDATE TO '1.2';

	-- now change the schema
	ALTER EXTENSION isn SET SCHEMA "extension'test";
COMMIT;

-- make sure that the object is distributed 
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname = 'isn');

-- show that the CREATE and ALTER EXTENSION commands are propagated as we committed the changes
SELECT run_command_on_workers($$SELECT extnamespace FROM pg_extension WHERE extname = 'isn'$$);
SELECT run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'isn'$$);

-- SET client_min_messages TO WARNING before executing a DROP EXTENSION statement
SET client_min_messages TO WARNING;
-- drop the extension finally
DROP EXTENSION isn CASCADE;
-- restore client_min_messages after DROP EXTENSION
RESET client_min_messages;

drop table ref_table;
drop table test_table;

-- now make sure that the reference tables depending on an extension can be succesfully created.
-- we should also ensure that we replicate this reference table (and hence the extension) 
-- to new nodes after calling master_add_node. 

-- but as we have only 2 ports in postgresql tests, let's remove one of the nodes first
SELECT master_remove_node('localhost', 57638);
select distinct logicalrelid from pg_dist_shard;

-- then create the extension
CREATE EXTENSION hstore;

-- show that the extension is created on existing worker
SELECT run_command_on_workers($$SELECT extnamespace FROM pg_extension WHERE extname = 'hstore'$$);
SELECT run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'hstore'$$);

-- now create the reference table
CREATE TABLE ref_table_2 (x hstore);
SELECT create_reference_table('ref_table_2');

-- and add the other node
SELECT master_add_node('localhost', 57638);

-- show that the extension is created on both existing and new nodes
SELECT run_command_on_workers($$SELECT extnamespace FROM pg_extension WHERE extname = 'hstore'$$);
SELECT run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'hstore'$$);

-- and similarly check for the reference table
select count(*) from pg_dist_partition where partmethod='n' and logicalrelid='ref_table_2'::regclass; 
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='ref_table_2'::regclass;

-- now test create extension in another transaction block but rollback this time
BEGIN;
	CREATE EXTENSION isn WITH VERSION '1.1' SCHEMA public;
ROLLBACK;

-- at the end of the transaction block, we did not create isn extension in coordinator or worker nodes as we rollback'ed
-- make sure that the extension is not distributed anymore
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname = 'isn');

-- and the extension is dropped on the workers
SELECT run_command_on_workers($$SELECT count(*) FROM pg_extension WHERE extname = 'isn'$$);

-- try setting the schema outside of the rollback'ed transaction block and get error
ALTER EXTENSION isn SET SCHEMA "public";

-- TODO: give a notice for the following commands saying that it is not
-- propagated to the workers. the user should run it manually on the workers 
-- ALTER EXTENSION name ADD member_object
-- ALTER EXTENSION name DROP member_object

-- drop multiple extensions at the same time
CREATE EXTENSION isn WITH VERSION '1.1' SCHEMA public;
-- let's create another extension locally
set citus.enable_ddl_propagation to 'off';
CREATE EXTENSION pg_buffercache;
set citus.enable_ddl_propagation to 'on';

-- SET client_min_messages TO WARNING before executing a DROP EXTENSION statement
SET client_min_messages TO WARNING;
-- restrict should fail but cascade should work
DROP EXTENSION hstore, isn RESTRICT; 
DROP EXTENSION hstore, isn CASCADE;
-- restore client_min_messages after DROP EXTENSION
RESET client_min_messages;

-- SET client_min_messages TO WARNING before executing a DROP EXTENSION statement
SET client_min_messages TO WARNING;
-- create/drop extension in a transaction block
BEGIN;
	CREATE EXTENSION hstore;
	DROP EXTENSION hstore CASCADE;
COMMIT;
-- restore client_min_messages after DROP EXTENSION
RESET client_min_messages;

-- make sure that the extension is not avaliable anymore as a distributed object
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname IN ('hstore', 'isn'));

-- but the schema should still be distributed
-- TODO: escape this SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_type WHERE typname = 'extension''test');

-- version
-- WITH schema
-- CASACADE
CREATE EXTENSION hstore;
DROP SCHEMA "extension'test" CASCADE;

-- make sure that dropping the schema removes the distributed object 
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname IN ('hstore'));

-- as we do not propagate DROP SCHEMA to workers for now, let's drop them manually
SELECT run_command_on_workers($$DROP SCHEMA "extension'test" CASCADE$$);

-- lastly, check functionality of EnsureSequentialModeForExtensionDDL
-- enable it and see that create command errors but continues its execution by changing citus.multi_shard_modify_mode TO 'off
SET citus.multi_shard_modify_mode TO 'parallel';

-- TODO: search_path exists but CREATE EXTENSION hstore fails claiming that "no schemas found to create in"
SHOW search_path;
CREATE EXTENSION hstore;

-- show that the CREATE EXTENSION command is propagated
SELECT run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'hstore'$$);

-- enable it and see that drop command errors but continues its execution by changing citus.multi_shard_modify_mode TO 'off
SET citus.multi_shard_modify_mode TO 'parallel';
DROP EXTENSION hstore;

-- show that the DROP EXTENSION command is propagated
SELECT run_command_on_workers($$SELECT count(*) FROM pg_extension WHERE extname = 'hstore'$$);

