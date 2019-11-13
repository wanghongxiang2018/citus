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
#include "distributed/commands.h"
#include "distributed/commands/utility_hook.h"
#include "distributed/deparser.h"
#include "distributed/function_utils.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/metadata_sync.h"
#include "distributed/metadata/distobject.h"
#include "nodes/makefuncs.h"
#include "utils/lsyscache.h"
#include "utils/builtins.h"


/* Local functions forward declarations for helper functions */
static char * ExtractNewExtensionVersion(Node *parsetree);
static void AddMissingFieldsCreateExtensionStmt(CreateExtensionStmt *stmt);
static char * GetDefaultExtensionVersion(char *extname);
static ReturnSetInfo * FunctionCallGetTupleStore(PGFunction function, Oid functionId);
static char * GetCurrentSchema(void);
static List * FilterDistributedExtensions(List *objects);
static List * ExtensionNameListToObjectAddresses(List *objects);
static bool ShouldPropagateExtensionCommand(Node *parseTree);
static bool IsDropCitusStmt(Node *parsetree);


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

	/* Extension management can only be done via coordinator node */
	EnsureCoordinator();

	/*
	 * Here we append "new_version" and "schema" fields to the "options" list
	 * if not specified to satisfy the version and schema consistency
	 * between worker nodes and the coordinator.
	 */
	AddMissingFieldsCreateExtensionStmt(createExtensionStmt);

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
 * Add "new_version" and "schema" DefElem items (if not specified in
 * statement) to "options" list before deparsing the statement.
 */
static void
AddMissingFieldsCreateExtensionStmt(CreateExtensionStmt *createExtensionStmt)
{
	List *optionsList = createExtensionStmt->options;

	const char *newVersion = GetCreateExtensionOption(optionsList, "new_version");
	const char *schemaName = GetCreateExtensionOption(optionsList, "schema");

	if (!newVersion)
	{
		char *extensionName = createExtensionStmt->extname;

		/*
		 * Get the latest available version of the extension from
		 * pg_available_extensions() to ensure version consistency
		 * between worker nodes and coordinator.
		 */
		char *extensionDefaultVersion = GetDefaultExtensionVersion(extensionName);

		Node *extensionVersionArg = (Node *) (makeString(extensionDefaultVersion));

		/* Set location to -1 as it is unknown */
		int location = -1;

		DefElem *newDefElement = makeDefElem("new_version", extensionVersionArg,
											 location);

		createExtensionStmt->options = lappend(createExtensionStmt->options,
											   newDefElement);
	}
	if (!schemaName)
	{
		char *currentSchemaName = GetCurrentSchema();

		Node *schemaNameArg = (Node *) (makeString(currentSchemaName));

		/* Set location to -1 as it is unknown */
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

	return targetStr;
}


/*
 * Get the latest available version of the extension with name "extname"
 * from pg_available_extensions udf.
 * Cannot return NULL, it'd error out as Postgres does if the version is
 * not avalible
 */
static char *
GetDefaultExtensionVersion(char *extname)
{
	TupleTableSlot *tupleTableSlot = NULL;
	ReturnSetInfo *resultSetInfo = NULL;
	FmgrInfo *fmgrAvaliableExtensions = (FmgrInfo *) palloc0(sizeof(FmgrInfo));
	Oid pgavaliableExtensionsOid =
		FunctionOidExtended("pg_catalog", "pg_available_extensions", 0, false);

	fmgr_info(pgavaliableExtensionsOid, fmgrAvaliableExtensions);

	resultSetInfo =
		FunctionCallGetTupleStore(fmgrAvaliableExtensions->fn_addr,
								  pgavaliableExtensionsOid);

	tupleTableSlot = MakeSingleTupleTableSlotCompat(resultSetInfo->setDesc,
													&TTSOpsMinimalTuple);

	while (true)
	{
		bool tuplePresent = false;
		bool isNull = false;

		const int extensionNameAttrNumber = 1;
		Datum extensionNameDatum = 0;
		Name extensionName = NULL;

		Datum defaultVersionDatum = 0;
		const int defaultVersionAttrNumber = 2;

		tuplePresent = tuplestore_gettupleslot(resultSetInfo->setResult,
											   true, false, tupleTableSlot);
		if (!tuplePresent)
		{
			/* no more rows */
			break;
		}

		extensionNameDatum =
			slot_getattr(tupleTableSlot, extensionNameAttrNumber, &isNull);
		if (isNull)
		{
			/* is this ever possible? Be on the safe side */
			continue;
		}

		extensionName = DatumGetName(extensionNameDatum);
		if (pg_strncasecmp(extensionName->data, extname, NAMEDATALEN) == 0)
		{
			defaultVersionDatum = slot_getattr(tupleTableSlot, defaultVersionAttrNumber,
											   &isNull);

			if (!isNull)
			{
				text *versionText = DatumGetTextP(defaultVersionDatum);

				return text_to_cstring(versionText);
			}
		}
	}

	/* couldn't find the name, this is not acceptable, Postgres would error as well */
	ereport(ERROR,
			(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
			 errmsg("invalid extension version name: \"%s\"", extname),
			 errdetail("Version names must not be empty.")));

	/* keep compilers happy */
	return NULL;
}


/*
 * FunctionCallGetTupleStore calls the given set-returning PGFunction and
 * returns the ResultSetInfo filled by the called function.
 */
static ReturnSetInfo *
FunctionCallGetTupleStore(PGFunction function, Oid functionId)
{
	LOCAL_FCINFO(fcinfo, 1);
	FmgrInfo flinfo;
	ReturnSetInfo *rsinfo = makeNode(ReturnSetInfo);
	EState *estate = CreateExecutorState();
	rsinfo->econtext = GetPerTupleExprContext(estate);
	rsinfo->allowedModes = SFRM_Materialize;

	fmgr_info(functionId, &flinfo);
	InitFunctionCallInfoData(*fcinfo, &flinfo, 0, InvalidOid, NULL, (Node *) rsinfo);

	(*function)(fcinfo);

	return rsinfo;
}


/*
 * Get the name of the schema that the coordinator would pick primarily
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

	if (search_path == NIL) /* nothing valid in search_path? */
	{
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_SCHEMA),
				 errmsg("no schema has been selected to create in")));
	}
	schemaOid = linitial_oid(search_path);
	schemaName = get_namespace_name(schemaOid);
	if (schemaName == NULL) /* recently-deleted namespace? */
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
	List *oldObjects = dropStmt->objects;

	List *distributedExtensions = NIL;
	List *distributedExtensionAddresses = NIL;

	List *commands = NIL;
	const char *deparsedStmt = NULL;

	ListCell *addressCell = NULL;

	if (!ShouldPropagateExtensionCommand((Node *) dropStmt))
	{
		return NIL;
	}

	/* Get distributed extensions to be dropped in worker nodes as well */
	distributedExtensions = FilterDistributedExtensions(oldObjects);

	if (list_length(distributedExtensions) <= 0)
	{
		/* no distributed extensions to drop */
		return NIL;
	}

	/* Extension management can only be done via coordinator node */
	EnsureCoordinator();

	distributedExtensionAddresses = ExtensionNameListToObjectAddresses(
		distributedExtensions);

	/* Unmark each distributed extension */
	foreach(addressCell, distributedExtensionAddresses)
	{
		ObjectAddress *address = (ObjectAddress *) lfirst(addressCell);
		UnmarkObjectDistributed(address);
	}

	/*
	 * Temporary swap the lists of objects to delete with the distributed
	 * objects and deparse to an sql statement for the workers.
	 * Then switch back to oldObjects to drop all specified extensions in
	 * coordinator after PlanDropExtensionStmt completes its execution.
	 */
	dropStmt->objects = distributedExtensions;
	deparsedStmt = DeparseTreeNode((Node *) dropStmt);

	dropStmt->objects = oldObjects;

	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) deparsedStmt,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


/*
 * Given the "objects" list of a DropStmt, return the distributed ones in
 * a Value(String) list.
 */
static List *
FilterDistributedExtensions(List *objects)
{
	List *result = NIL;

	bool missingOk = true;
	ListCell *objectCell = NULL;

	foreach(objectCell, objects)
	{
		char *extensionName = strVal(lfirst(objectCell));

		ObjectAddress *address = palloc0(sizeof(ObjectAddress));

		Oid extensionoid = get_extension_oid(extensionName, missingOk);

		if (!OidIsValid(extensionoid))
		{
			continue;
		}

		ObjectAddressSet(*address, ExtensionRelationId, extensionoid);

		if (!IsObjectDistributed(address))
		{
			continue;
		}

		result = lappend(result, makeString(extensionName));
	}

	return result;
}


/*
 * Given the "objects" list of a DropStmt, return the object addresses in
 * an ObjectAddress list.
 * Callers of this function should ensure that all the objects in the list
 * are valid and distributed.
 */
static List *
ExtensionNameListToObjectAddresses(List *objects)
{
	List *result = NIL;

	bool missingOk = true;
	ListCell *objectCell = NULL;

	foreach(objectCell, objects)
	{
		const char *extensionName = strVal(lfirst(objectCell));

		ObjectAddress *address = palloc0(sizeof(ObjectAddress));

		Oid extensionoid = get_extension_oid(extensionName, missingOk);

		ObjectAddressSet(*address, ExtensionRelationId, extensionoid);

		result = lappend(result, address);
	}

	return result;
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
	if (IsDropCitusStmt(parseTree))
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
IsCreateAlterCitusStmt(Node *parsetree)
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
IsDropCitusStmt(Node *parsetree)
{
	ListCell *objectCell = NULL;

	/* If it is not a DropStmt, it is needless to search for citus */
	if (!IsA(parsetree, DropStmt))
	{
		return false;
	}

	/* Now that we have a DropStmt, check if citus is among the objects to dropped */
	foreach(objectCell, ((DropStmt *) parsetree)->objects)
	{
		char *extensionName = strVal(lfirst(objectCell));

		if (strncmp(extensionName, "citus", NAMEDATALEN) == 0)
		{
			return true;
		}
	}

	return false;
}
