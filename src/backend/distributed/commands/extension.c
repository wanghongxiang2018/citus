/*-------------------------------------------------------------------------
 *
 * extension.c
 *    Commands for creating and altering extensions.
 *
 * Copyright (c) Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "citus_version.h"
#include "catalog/pg_extension_d.h"
#include "commands/extension.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/distobject.h"
#include "distributed/multi_executor.h"
#include "distributed/relation_access_tracking.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"


/* Local functions forward declarations for helper functions */
static char * ExtractNewExtensionVersion(Node *parseTree);
static void AddSchemaFieldIfMissing(CreateExtensionStmt *stmt);
static char * GetCurrentSchema(void);
static void EnsureSequentialModeForExtensionDDL(void);
static List * FilterDistributedExtensions(List *extensionObjectList);
static List * ExtensionNameListToObjectAddressList(List *extensionObjectList);
static bool ShouldPropagateExtensionCommand(Node *parseTree);
static bool IsDropCitusStmt(Node *parseTree);
static Node * RecreateExtensionStmt(Oid extensionOid);


/*
 * ErrorIfUnstableCreateOrAlterExtensionStmt compares CITUS_EXTENSIONVERSION
 * and version given CREATE/ALTER EXTENSION statement will create/update to. If
 * they are not same in major or minor version numbers, this function errors
 * out. It ignores the schema version.
 */
void
ErrorIfUnstableCreateOrAlterExtensionStmt(Node *parseTree)
{
	char *newExtensionVersion = ExtractNewExtensionVersion(parseTree);

	if (newExtensionVersion != NULL)
	{
		/*  explicit version provided in CREATE or ALTER EXTENSION UPDATE; verify */
		if (!MajorVersionsCompatible(newExtensionVersion, CITUS_EXTENSIONVERSION))
		{
			ereport(ERROR, (errmsg("specified version incompatible with loaded "
								   "Citus library"),
							errdetail("Loaded library requires %s, but %s was specified.",
									  CITUS_MAJORVERSION, newExtensionVersion),
							errhint("If a newer library is present, restart the database "
									"and try the command again.")));
		}
	}
	else
	{
		/*
		 * No version was specified, so PostgreSQL will use the default_version
		 * from the citus.control file.
		 */
		CheckAvailableVersion(ERROR);
	}
}


/*
 * ExtractNewExtensionVersion returns the new extension version specified by
 * a CREATE or ALTER EXTENSION statement. Other inputs are not permitted. This
 * function returns NULL for statements with no explicit version specified.
 */
static char *
ExtractNewExtensionVersion(Node *parseTree)
{
	char *newVersion = NULL;
	List *optionsList = NIL;
	ListCell *optionsCell = NULL;

	if (IsA(parseTree, CreateExtensionStmt))
	{
		optionsList = ((CreateExtensionStmt *) parseTree)->options;
	}
	else if (IsA(parseTree, AlterExtensionStmt))
	{
		optionsList = ((AlterExtensionStmt *) parseTree)->options;
	}
	else
	{
		/* input must be one of the two above types */
		Assert(false);
	}

	foreach(optionsCell, optionsList)
	{
		DefElem *defElement = (DefElem *) lfirst(optionsCell);
		if (strncmp(defElement->defname, "new_version", NAMEDATALEN) == 0)
		{
			newVersion = strVal(defElement->arg);
			break;
		}
	}

	/* return target string safely */
	if (newVersion)
	{
		return pstrdup(newVersion);
	}
	else
	{
		return NULL;
	}
}


/*
 * PlanCreateExtensionStmt is called during the creation of an extension.
 * It is executed before the statement is applied locally.
 * We decide if the extension needs to be replicated to the worker, and
 * if that is the case return a list of DDLJob's that describe how and
 * where the extension needs to be created.
 */
List *
PlanCreateExtensionStmt(CreateExtensionStmt *createExtensionStmt, const char *queryString)
{
	List *commands = NIL;
	const char *createExtensionStmtSql = NULL;

	if (!ShouldPropagateExtensionCommand((Node *) createExtensionStmt))
	{
		return NIL;
	}

	/* extension management can only be done via coordinator node */
	EnsureCoordinator();

	/*
	 * Make sure that no new nodes are added after this point until the end of the
	 * transaction by taking a RowShareLock on pg_dist_node, which conflicts with the
	 * ExclusiveLock taken by master_add_node.
	 * This guarantees that all active nodes will have the extension, because they will
	 * either get it now, or get it in master_add_node after this transaction finishes and
	 * the pg_dist_object record becomes visible.
	 */
	LockRelationOid(DistNodeRelationId(), RowShareLock);

	/*
	 * Make sure that the current transaction is already in sequential mode,
	 * or can still safely be put in sequential mode
	 */
	EnsureSequentialModeForExtensionDDL();

	/*
	 * Here we append "schema" field to the "options" list (if not specified)
	 * to satisfy the version consistency between worker nodes and the coordinator.
	 */
	AddSchemaFieldIfMissing(createExtensionStmt);

	createExtensionStmtSql = DeparseTreeNode((Node *) createExtensionStmt);

	/*
	 * To prevent recursive propagation in mx architecture, we disable ddl
	 * propagation before sending the command to workers.
	 */
	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) createExtensionStmtSql,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * Add DefElem item for "schema" (if not specified in statement) to "options"
 * list before deparsing the statement to satisfy the version consistency
 * between worker nodes and the coordinator.
 */
static void
AddSchemaFieldIfMissing(CreateExtensionStmt *createExtensionStmt)
{
	List *optionsList = createExtensionStmt->options;

	const char *schemaName = GetCreateExtensionOption(optionsList, "schema");

	if (!schemaName)
	{
		char *currentSchemaName = GetCurrentSchema();

		Node *schemaNameArg = (Node *) makeString(currentSchemaName);

		/* set location to -1 as it is unknown */
		int location = -1;

		DefElem *newDefElement = makeDefElem("schema", schemaNameArg, location);

		createExtensionStmt->options = lappend(createExtensionStmt->options,
											   newDefElement);
	}
}


/*
 * Utility function to fetch the string value of DefElem node with "defname"
 * from "options" list
 */
const char *
GetCreateExtensionOption(List *extensionOptions, const char *defname)
{
	const char *targetStr = NULL;

	ListCell *defElemCell = NULL;

	foreach(defElemCell, extensionOptions)
	{
		DefElem *defElement = (DefElem *) lfirst(defElemCell);

		if (IsA(defElement, DefElem) && strncmp(defElement->defname, defname,
												NAMEDATALEN) == 0)
		{
			targetStr = strVal(defElement->arg);
			break;
		}
	}

	/* return target string safely */
	if (targetStr)
	{
		return pstrdup(targetStr);
	}
	else
	{
		return NULL;
	}
}


/*
 * Get the name of the schema that the postgres would pick primarily
 * for a CREATE EXTENSION statement that does not include "WITH SCHEMA"
 * clause.
 * Cannot return NULL, errors out as Postgres would do if NULL.
 */
static char *
GetCurrentSchema(void)
{
	/*
	 * Neither user nor author of the extension specified schema; use the
	 * current default creation namespace, which is the first explicit
	 * entry in the search_path.
	 */
	List *search_path = fetch_search_path(false);
	Oid schemaOid = InvalidOid;
	char *schemaName = NULL;

	/* nothing valid in search_path? */
	if (search_path == NIL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("no schema has been selected to create in")));
	}
	schemaOid = linitial_oid(search_path);
	schemaName = get_namespace_name(schemaOid);

	/* recently-deleted namespace? */
	if (schemaName == NULL)
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("no schema has been selected to create in")));
	}

	list_free(search_path);

	return pstrdup(schemaName);
}


/*
 * ProcessCreateExtensionStmt is executed after the extension has been
 * created locally and before we create it on the worker nodes.
 * As we now have access to ObjectAddress of the extension that is just
 * created, we can mark it as distributed to make sure that its
 * dependencies exist on all nodes.
 */
void
ProcessCreateExtensionStmt(CreateExtensionStmt *createExtensionStmt, const
						   char *queryString)
{
	const ObjectAddress *extensionAddress = NULL;

	if (!ShouldPropagateExtensionCommand((Node *) createExtensionStmt))
	{
		return;
	}

	extensionAddress = GetObjectAddressFromParseTree((Node *) createExtensionStmt, false);

	EnsureDependenciesExistsOnAllNodes(extensionAddress);

	MarkObjectDistributed(extensionAddress);
}


/*
 * PlanDropExtensionStmt is called to drop extension(s) in coordinator and
 * in worker nodes if distributed before.
 * We first ensure that we keep only the distributed ones before propagating
 * the statement to worker nodes.
 * If no extensions in the drop list are distributed, then no calls will
 * be made to the workers.
 */
List *
PlanDropExtensionStmt(DropStmt *dropStmt, const char *queryString)
{
	List *allDroppedExtensions = dropStmt->objects;

	List *distributedExtensions = NIL;
	List *distributedExtensionAddresses = NIL;

	List *commands = NIL;
	const char *deparsedStmt = NULL;

	ListCell *addressCell = NULL;

	if (!ShouldPropagateExtensionCommand((Node *) dropStmt))
	{
		return NIL;
	}

	/* extension management can only be done via coordinator node */
	EnsureCoordinator();

	/*
	 * Make sure that the current transaction is already in sequential mode,
	 * or can still safely be put in sequential mode
	 */
	EnsureSequentialModeForExtensionDDL();

	/* get distributed extensions to be dropped in worker nodes as well */
	distributedExtensions = FilterDistributedExtensions(allDroppedExtensions);

	if (list_length(distributedExtensions) <= 0)
	{
		/* no distributed extensions to drop */
		return NIL;
	}

	distributedExtensionAddresses = ExtensionNameListToObjectAddressList(
		distributedExtensions);

	/* unmark each distributed extension */
	foreach(addressCell, distributedExtensionAddresses)
	{
		ObjectAddress *address = (ObjectAddress *) lfirst(addressCell);
		UnmarkObjectDistributed(address);
	}

	/*
	 * Temporary swap the lists of objects to delete with the distributed
	 * objects and deparse to an sql statement for the workers.
	 * Then switch back to allDroppedExtensions to drop all specified
	 * extensions in coordinator after PlanDropExtensionStmt completes
	 * its execution.
	 */
	dropStmt->objects = distributedExtensions;
	deparsedStmt = DeparseTreeNode((Node *) dropStmt);

	dropStmt->objects = allDroppedExtensions;

	/*
	 * To prevent recursive propagation in mx architecture, we disable ddl
	 * propagation before sending the command to workers.
	 */
	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) deparsedStmt,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * EnsureSequentialModeForExtensionDDL makes sure that the current transaction is already in
 * sequential mode, or can still safely be put in sequential mode, it errors if that is
 * not possible. The error contains information for the user to retry the transaction with
 * sequential mode set from the beginnig.
 *
 * As extensions are node scoped objects there exists only 1 instance of the extension used by
 * potentially multiple shards. To make sure all shards in the transaction can interact
 * with the extension the extension needs to be visible on all connections used by the transaction,
 * meaning we can only use 1 connection per node.
 */
static void
EnsureSequentialModeForExtensionDDL(void)
{
	if (ParallelQueryExecutedInTransaction())
	{
		ereport(ERROR, (errmsg("cannot create extension because there was a "
							   "parallel operation on a distributed table in the "
							   "transaction"),
						errdetail("When creating a distributed extension, Citus needs to "
								  "perform all operations over a single connection per "
								  "node to ensure consistency."),
						errhint("Try re-running the transaction with "
								"\"SET LOCAL citus.multi_shard_modify_mode TO "
								"\'sequential\';\"")));
	}

	ereport(DEBUG1, (errmsg("switching to sequential query execution mode"),
					 errdetail(
						 "A distributed extension is created. To make sure subsequent "
						 "commands see the type correctly we need to make sure to "
						 "use only one connection for all future commands")));
	SetLocalMultiShardModifyModeToSequential();
}


/*
 * Given the "objects" list of a DropStmt, return the distributed objects in a
 * list having the format of a "DropStmt.objects" list (a list of string "Value"s).
 */
static List *
FilterDistributedExtensions(List *extensionObjectList)
{
	List *extensionNameList = NIL;

	bool missingOk = true;
	ListCell *objectCell = NULL;

	foreach(objectCell, extensionObjectList)
	{
		char *extensionName = strVal(lfirst(objectCell));

		ObjectAddress *address = palloc0(sizeof(ObjectAddress));

		Oid extensionOid = get_extension_oid(extensionName, missingOk);

		if (!OidIsValid(extensionOid))
		{
			continue;
		}

		ObjectAddressSet(*address, ExtensionRelationId, extensionOid);

		if (!IsObjectDistributed(address))
		{
			continue;
		}

		extensionNameList = lappend(extensionNameList, makeString(extensionName));
	}

	return extensionNameList;
}


/*
 * Given the "objects" list of a DropStmt, return the object addresses in
 * an ObjectAddress list.
 * Callers of this function should ensure that all the objects in the list
 * are valid and distributed.
 */
static List *
ExtensionNameListToObjectAddressList(List *extensionObjectList)
{
	List *extensionObjectAddressList = NIL;

	/*
	 * We set missingOk to false as we assume all the objects in
	 * extensionObjectList list are valid and distributed.
	 */
	bool missingOk = false;

	ListCell *objectCell = NULL;

	foreach(objectCell, extensionObjectList)
	{
		const char *extensionName = strVal(lfirst(objectCell));

		ObjectAddress *address = palloc0(sizeof(ObjectAddress));

		Oid extensionOid = get_extension_oid(extensionName, missingOk);

		ObjectAddressSet(*address, ExtensionRelationId, extensionOid);

		extensionObjectAddressList = lappend(extensionObjectAddressList, address);
	}

	return extensionObjectAddressList;
}


/*
 * If we disabled object propagation, then we should not propagate anything.
 * Also, if extension command is run for/on citus, leave the rest to standard
 * utility hook.
 */
static bool
ShouldPropagateExtensionCommand(Node *parseTree)
{
	if (!EnableDependencyCreation)
	{
		return false;
	}

	if (IsCreateAlterCitusStmt(parseTree))
	{
		return false;
	}
	else if (IsDropCitusStmt(parseTree))
	{
		return false;
	}

	return true;
}


/*
 * IsCreateAlterCitusStmt returns whether a given utility is a CREATE or ALTER
 * EXTENSION statement which references the citus extension. This function
 * returns false for all other inputs.
 */
bool
IsCreateAlterCitusStmt(Node *parseTree)
{
	char *extensionName = NULL;

	if (IsA(parseTree, CreateExtensionStmt))
	{
		extensionName = ((CreateExtensionStmt *) parseTree)->extname;
	}
	else if (IsA(parseTree, AlterExtensionStmt))
	{
		extensionName = ((AlterExtensionStmt *) parseTree)->extname;
	}
	else
	{
		/*
		 * If it is not a CreateExtensionStmt or AlterExtensionStmt,
		 * it does not matter if the it is about citus
		 */
		return false;
	}

	/*
	 * Now that we have CreateExtensionStmt or AlterExtensionStmt,
	 * check if it is run for/on citus
	 */
	return (strncmp(extensionName, "citus", NAMEDATALEN) == 0);
}


/*
 * Iterate objects to be dropped in a drop statement and try to find citus there
 */
static bool
IsDropCitusStmt(Node *parseTree)
{
	ListCell *objectCell = NULL;

	/* if it is not a DropStmt, it is needless to search for citus */
	if (!IsA(parseTree, DropStmt))
	{
		return false;
	}

	/* now that we have a DropStmt, check if citus is among the objects to dropped */
	foreach(objectCell, ((DropStmt *) parseTree)->objects)
	{
		char *extensionName = strVal(lfirst(objectCell));

		if (strncmp(extensionName, "citus", NAMEDATALEN) == 0)
		{
			return true;
		}
	}

	return false;
}


/*
 * CreateTypeDDLCommandsIdempotent returns a list of DDL statements (const char *) to be
 * executed on a node to recreate the extension addressed by the extensionAddress.
 */
List *
CreateExtensionDDLCommand(const ObjectAddress *extensionAddress)
{
	List *ddlCommands = NIL;
	const char *ddlCommand = NULL;

	Node *stmt = NULL;

	/* generate a statement for creation of the extension in "if not exists" construct */
	stmt = RecreateExtensionStmt(extensionAddress->objectId);

	/* capture ddl command for the create statement */
	ddlCommand = DeparseTreeNode(stmt);

	/*
	 * To prevent recursive propagation in mx architecture, we disable ddl
	 * propagation before sending the command to workers.
	 */
	ddlCommands = list_make3(DISABLE_DDL_PROPAGATION,
							 (void *) ddlCommand,
							 ENABLE_DDL_PROPAGATION);

	return ddlCommands;
}


/*
 * RecreateEnumStmt returns a parsetree for a CREATE EXTENSION statement that would
 * recreate the given extension on a new node.
 */
static Node *
RecreateExtensionStmt(Oid extensionOid)
{
	CreateExtensionStmt *stmt = makeNode(CreateExtensionStmt);

	char *extensionName = get_extension_name(extensionOid);

	/* set extension name and if_not_exists fields */
	stmt->extname = extensionName;
	stmt->if_not_exists = true;

	/* append the missing schema name DefElem */
	AddSchemaFieldIfMissing(stmt);

	return (Node *) stmt;
}
