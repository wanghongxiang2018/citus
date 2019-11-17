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

--  make sure that the table is also distributed now
SELECT count(*) from pg_dist_partition where logicalrelid='extension''test.test_table'::regclass;

CREATE TYPE two_hstores AS (hstore_1 hstore, hstore_2 hstore);

-- verify that the type that depends on the extension is also marked as distributed
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_type WHERE typname = 'two_hstores' AND typnamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'extension''test'));

-- now try to run CREATE EXTENSION within a transction block and observe it fails
BEGIN;
	CREATE EXTENSION isn WITH SCHEMA public VERSION '1.1';

  -- now, try create a reference table relying on the data types
  -- this should not succeed as we do not distribute extension commands within transaction blocks
	CREATE TABLE ref_table (a public.issn);
	SELECT create_reference_table('ref_table');
COMMIT;

-- make sure that the extension could not be distributed as we run create extension in a transaction block
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname = 'isn');
SELECT run_command_on_workers($$SELECT count(*) FROM pg_extension WHERE extname = 'isn'$$);

-- let's do the stuff target in above transaction block as they are rollback'ed because of the failure

CREATE EXTENSION isn WITH SCHEMA public VERSION '1.1';

CREATE TABLE ref_table (a public.issn);
-- now, create a reference table relying on the data types
SELECT create_reference_table('ref_table');

-- before updating the version, ensure the current version
SELECT run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'isn'$$);

-- now, update to a newer version
ALTER EXTENSION isn UPDATE TO '1.2';

-- before changing the schema, ensure the current schmea
SELECT run_command_on_workers($$SELECT nspname from pg_namespace where oid=(SELECT extnamespace FROM pg_extension WHERE extname = 'isn')$$);

-- now change the schema
ALTER EXTENSION isn SET SCHEMA public;

-- switch back to public schema as we set extension's schema to public
SET search_path TO public;

-- make sure that the extension is distributed
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname = 'isn');

-- show that the CREATE and ALTER EXTENSION commands are propagated
SELECT run_command_on_workers($$SELECT nspname from pg_namespace where oid=(SELECT extnamespace FROM pg_extension WHERE extname = 'isn')$$);
SELECT run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'isn'$$);

-- SET client_min_messages TO WARNING before executing a DROP EXTENSION statement
SET client_min_messages TO WARNING;
-- drop the extension finally
DROP EXTENSION isn CASCADE;
-- restore client_min_messages after DROP EXTENSION
RESET client_min_messages;

-- now make sure that the reference tables depending on an extension can be succesfully created.
-- we should also ensure that we replicate this reference table (and hence the extension) 
-- to new nodes after calling master_activate_node. 

-- now, first drop hstore and existing objects before next test
DROP EXTENSION hstore CASCADE;

-- but as we have only 2 ports in postgresql tests, let's remove one of the nodes first
-- before remove, first remove the existing relations (due to the other tests)
\d
DROP TABLE ref_table;
DROP TABLE test_table;
SELECT master_remove_node('localhost', 57638);

-- then create the extension
CREATE EXTENSION hstore;

-- show that the extension is created on existing worker
SELECT run_command_on_workers($$SELECT count(extnamespace) FROM pg_extension WHERE extname = 'hstore'$$);
SELECT run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'hstore'$$);

-- now create the reference table
CREATE TABLE ref_table_2 (x hstore);
SELECT create_reference_table('ref_table_2');

-- and add the other node
SELECT master_add_node('localhost', 57638);

-- show that the extension is created on both existing and new node
SELECT run_command_on_workers($$SELECT count(extnamespace) FROM pg_extension WHERE extname = 'hstore'$$);
SELECT run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'hstore'$$);

-- and similarly check for the reference table
select count(*) from pg_dist_partition where partmethod='n' and logicalrelid='ref_table_2'::regclass; 
SELECT count(*) FROM pg_dist_shard WHERE logicalrelid='ref_table_2'::regclass;

-- now test create extension in another transaction block but rollback this time
BEGIN;
	CREATE EXTENSION isn WITH VERSION '1.1' SCHEMA public;
ROLLBACK;

-- at the end of the transaction block, we did not create isn extension in coordinator or worker nodes as we rollback'ed
-- make sure that the extension is not distributed
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname = 'isn');

-- and the extension does not exist on workers
SELECT run_command_on_workers($$SELECT count(*) FROM pg_extension WHERE extname = 'isn'$$);

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
DROP EXTENSION pg_buffercache, isn RESTRICT; 
-- but this should work
DROP EXTENSION pg_buffercache, isn CASCADE;
-- restore client_min_messages after DROP EXTENSION
RESET client_min_messages;

-- SET client_min_messages TO WARNING before executing a DROP EXTENSION statement
SET client_min_messages TO WARNING;

-- drop extension in a transaction block, should not be distributed to workers
BEGIN;
	DROP EXTENSION hstore CASCADE;
COMMIT;

-- finally, drop the extension
DROP EXTENSION hstore;

-- restore client_min_messages after DROP EXTENSION
RESET client_min_messages;

-- make sure that the extension is not avaliable anymore as a distributed object
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname IN ('hstore', 'isn'));

-- but the schema should still be distributed
-- TODO: escape this SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_type WHERE typname = 'extension''test');

-- version
-- WITH schema
-- CASACADE
CREATE EXTENSION hstore WITH SCHEMA "extension'test";
DROP SCHEMA "extension'test" CASCADE;

-- make sure that dropping the schema removes the distributed object 
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname IN ('hstore'));

-- as we do not propagate DROP SCHEMA to workers, let's drop them manually
SELECT run_command_on_workers($$DROP SCHEMA "extension'test" CASCADE$$);

-- lastly, check functionality of EnsureSequentialModeForExtensionDDL
-- enable it and see that create command errors but continues its execution by changing citus.multi_shard_modify_mode TO 'off
SET citus.multi_shard_modify_mode TO 'parallel';

CREATE EXTENSION hstore;

-- show that the CREATE EXTENSION command is propagated
SELECT run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'hstore'$$);

-- enable it and see that drop command errors but continues its execution by changing citus.multi_shard_modify_mode TO 'off
SET citus.multi_shard_modify_mode TO 'parallel';
DROP EXTENSION hstore;

-- show that the DROP EXTENSION command is propagated
SELECT run_command_on_workers($$SELECT count(*) FROM pg_extension WHERE extname = 'hstore'$$);

--finally, drop ref_table_2 to avoid affecting the other regression tests
DROP TABLE ref_table_2;

