-- master add node

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


-- now CREATE EXTENSION within a transction block and with version defined
BEGIN;

	CREATE EXTENSION isn WITH VERSION '1.1' SCHEMA public;

	-- make sure that the object is distributed 
	SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname = 'isn');

	-- now, create a reference table relying on the data types
	CREATE TABLE ref_table (a public.issn);
	SELECT create_reference_table('ref_table');

	-- and the extension is created on the workers -- TODO: they return 0 as we are in the transaction block
	SELECT run_command_on_workers($$SELECT count(*) FROM pg_extension WHERE extname = 'isn'$$);


	-- now, update to a newer version
	ALTER EXTENSION isn UPDATE TO '1.2';

	-- now change the schema
	ALTER EXTENSION isn SET SCHEMA "extension'test";

	-- show that the ALTER EXTENSION commands are propagated
	SELECT run_command_on_workers($$SELECT extnamespace FROM pg_extension WHERE extname = 'isn'$$);
	SELECT run_command_on_workers($$SELECT extversion FROM pg_extension WHERE extname = 'isn'$$);
ROLLBACK;

-- at the end of the transaction block, we did not create isn extension in coordinator or worker nodes as we rollback'ed

-- make sure that the extension is not distributed anymore
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname = 'isn');

-- and the extension is dropped on the workers
SELECT run_command_on_workers($$SELECT count(*) FROM pg_extension WHERE extname = 'isn'$$);

-- try setting the schema outside of the transaction block, TODO: alters in coordinator but not in workers as it is not implemented yed
ALTER EXTENSION hstore SET SCHEMA "public";

-- TODO: give a notice for the following commands saying that it is not
-- propagated to the workers. the user should run it manually on the workers 
-- ALTER EXTENSION name ADD member_object
-- ALTER EXTENSION name DROP member_object

DROP EXTENSION hstore CASCADE;

-- drop multiple extensions at the same time
CREATE EXTENSION hstore;
CREATE EXTENSION isn WITH VERSION '1.1' SCHEMA public;
-- lets another extension locally
set citus.enable_ddl_propagation to 'off';
CREATE EXTENSION pg_buffercache;
set citus.enable_ddl_propagation to 'on';

--DROP EXTENSION hstore, isn RESTRICT; -- TODO: do we need this ? restrict fail etti, depend eden tablo olsun
DROP EXTENSION hstore, isn CASCADE;

-- create/drop extension in a transaction block
BEGIN;
	CREATE EXTENSION hstore;
	DROP EXTENSION hstore CASCADE;
COMMIT;

-- make sure that the extension is not avaliable anymore as a distributed object
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname IN ('hstore', 'isn'));

-- but the schema should still be distributed
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_type WHERE pg_type = 'extension''test');

-- version
-- WITH schema
-- CASACADE
CREATE EXTENSION hstore;
DROP SCHEMA "extension'test" CASCADE;

-- make sure that dropping the schema removes the distributed object 
SELECT count(*) FROM citus.pg_dist_object WHERE objid = (SELECT oid FROM pg_extension WHERE extname IN ('hstore'));

-- as we do not propagate DROP SCHEMA to workers for now, let's drop them manually
SELECT run_command_on_workers($$DROP SCHEMA "extension'test" CASCADE$$);

