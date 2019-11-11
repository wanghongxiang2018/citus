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
#include "nodes/makefuncs.h"
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/worker_transaction.h"
#include "distributed/metadata/distobject.h"
#include "server/access/genam.h"
#include "server/commands/extension.h"
#include "server/catalog/pg_extension_d.h"
#include "server/catalog/pg_namespace_d.h"
#include "server/catalog/objectaddress.h"
#include "server/nodes/parsenodes.h"
#include "server/nodes/pg_list.h"
#include "server/utils/fmgroids.h"
#include "server/postgres.h"
#include "utils/lsyscache.h"


/* Local functions forward declarations for helper functions */
static char * ExtractNewExtensionVersion(Node *parsetree);
static bool ShouldPropagateExtensionCreate(void);
static void AddMissingFieldsCreateExtensionStmt(CreateExtensionStmt *stmt);
static const ObjectAddress * GetSchemaAddress(const char * schemaName);

/*
 * IsCitusExtensionStmt returns whether a given utility is a CREATE or ALTER
 * EXTENSION statement which references the citus extension. This function
 * returns false for all other inputs.
 */
bool
IsCitusExtensionStmt(Node *parsetree)
{
	char *extensionName = "";

	if (IsA(parsetree, CreateExtensionStmt))
	{
		extensionName = ((CreateExtensionStmt *) parsetree)->extname;
	}
	else if (IsA(parsetree, AlterExtensionStmt))
	{
		extensionName = ((AlterExtensionStmt *) parsetree)->extname;
	}

	return (strcmp(extensionName, "citus") == 0);
}


/*
 * ErrorIfUnstableCreateOrAlterExtensionStmt compares CITUS_EXTENSIONVERSION
 * and version given CREATE/ALTER EXTENSION statement will create/update to. If
 * they are not same in major or minor version numbers, this function errors
 * out. It ignores the schema version.
 */
void
ErrorIfUnstableCreateOrAlterExtensionStmt(Node *parsetree)
{
	char *newExtensionVersion = ExtractNewExtensionVersion(parsetree);

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
ExtractNewExtensionVersion(Node *parsetree)
{
	char *newVersion = NULL;
	List *optionsList = NIL;
	ListCell *optionsCell = NULL;

	if (IsA(parsetree, CreateExtensionStmt))
	{
		optionsList = ((CreateExtensionStmt *) parsetree)->options;
	}
	else if (IsA(parsetree, AlterExtensionStmt))
	{
		optionsList = ((AlterExtensionStmt *) parsetree)->options;
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

	return newVersion;
}


List *
PlanCreateExtensionStmt(CreateExtensionStmt *stmt, const char *queryString)
{
	// TODO: @onurctirtir which lock should I take ??

	List *commands = NIL;
	const char *createExtensionStmtSql = NULL;

	if (!ShouldPropagateExtensionCreate())
	{
		return NIL;
	}

	EnsureCoordinator();

	AddMissingFieldsCreateExtensionStmt(stmt);

	createExtensionStmtSql = DeparseTreeNode((Node*) stmt);
	// TODO: @onurctirtir not sure about below call ?
	//EnsureSequentialModeForTypeDDL();

	/* TODO: @onurctirtir, to prevent recursion with mx we disable ddl propagation, should we ? Ask to Onder */
	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) createExtensionStmtSql,
						  ENABLE_DDL_PROPAGATION);

	// DEBUG
	elog(DEBUG1, queryString);
	elog(DEBUG1, createExtensionStmtSql);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}

void
ProcessCreateExtensionStmt(CreateExtensionStmt *stmt, const char *queryString)
{
	const ObjectAddress *extensionAddress = NULL, *schemaAddress = NULL;

	CreateExtensionOptions fetchedOptions;
		
	if (!ShouldPropagateExtensionCreate()) {
		return;
	}

	extensionAddress = GetObjectAddressFromParseTree((Node *) stmt, false);

	EnsureDependenciesExistsOnAllNodes(extensionAddress);

	MarkObjectDistributed(extensionAddress);

    fetchedOptions = FetchCreateExtensionOptionList(stmt);
	schemaAddress = GetSchemaAddress(fetchedOptions.schemaName);
	MarkObjectDistributed(schemaAddress);
}

List *
ProcessDropExtensionStmt(DropStmt *stmt, const char *queryString)
{
	List *commands = NIL;
	bool missingOk = true;
	ListCell *dropExtensionEntry = NULL;

	// iterate each extension in drop stmt 
	foreach(dropExtensionEntry, stmt->objects)
	{
		char *extensionName = strVal(lfirst(dropExtensionEntry));

		ObjectAddress *address = palloc0(sizeof(ObjectAddress));

		Oid extensionoid = get_extension_oid(extensionName, missingOk);
		
		ObjectAddressSet(*address, ExtensionRelationId, extensionoid);

		elog(DEBUG1, extensionName);

		if (extensionoid == InvalidOid || !IsObjectDistributed(address))
		{
			continue;
		}

		UnmarkObjectDistributed(address);

		// TODO: unmark schema as distributed as well

		commands = lappend(commands, (void *) queryString);

		elog(DEBUG1, queryString);
	}

	return NodeDDLTaskList(ALL_WORKERS, commands);
}

static bool
ShouldPropagateExtensionCreate(void)
{
	if (!EnableDependencyCreation)
	{
		/*
		 * if we disabled object propagation, then we should not propagate anything
		 */
		return false;
	}

	return true;
}


/*
 * We process and add missing fields to CreateExtensionStmt before deparse as 
 * we alse need parseTree of modified query in PlanCreateExtensionStmt call.
 */
static void
AddMissingFieldsCreateExtensionStmt(CreateExtensionStmt *stmt)
{
	/* we may need to qualiy DefElem list */
	List *optionsList = stmt->options;
	
	/* Check if the above fields are specified in original statement */
	CreateExtensionOptions fetchedOptions = FetchCreateExtensionOptionList(stmt);

	if (!fetchedOptions.new_version)
	{
		DefElem *newDefElement = makeDefElem("new_version", (Node*)(makeString("version_num")), -1);
		optionsList = lappend(optionsList, newDefElement);

		// TODO: find the latest version available in coordinator and append it
	}	
	if (!fetchedOptions.schemaName)
	{
		DefElem *newDefElement = makeDefElem("schema", (Node*)(makeString("schema_name")), -1);
		optionsList = lappend(optionsList, newDefElement);

		// TODO: append current schema
	}
}

static const ObjectAddress * GetSchemaAddress(const char * schemaName)
{
	ObjectAddress *address = palloc0(sizeof(ObjectAddress));
	Oid pg_namespace_oid = get_namespace_oid(schemaName, true);

	// TODO: using NamespaceRelationId is appropriate ??
	ObjectAddressSet(*address, NamespaceRelationId, pg_namespace_oid);

	return address;
}
