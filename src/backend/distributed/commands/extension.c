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
static bool ShouldPropagateCreateDropExtension(void);
static void AddMissingFieldsCreateExtensionStmt(CreateExtensionStmt *stmt);
static List * FilterNameListForDistributedExtensions(List *objects);
static List * ExtensionNameListToObjectAddresses(List *objects);
static char * GetCurrentSchema(void);
static char * GetDefaultExtensionVersion(char *extname);
static ReturnSetInfo * FunctionCallGetTupleStore(PGFunction function, Oid functionId);

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
	List *commands = NIL;
	const char *createExtensionStmtSql = NULL;

	if (!ShouldPropagateCreateDropExtension())
	{
		return NIL;
	}

	EnsureCoordinator();

	AddMissingFieldsCreateExtensionStmt(stmt);

	createExtensionStmtSql = DeparseTreeNode((Node *) stmt);

	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) createExtensionStmtSql,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


void
ProcessCreateExtensionStmt(CreateExtensionStmt *stmt, const char *queryString)
{
	const ObjectAddress *extensionAddress = NULL;

	if (!ShouldPropagateCreateDropExtension())
	{
		return;
	}

	extensionAddress = GetObjectAddressFromParseTree((Node *) stmt, false);

	EnsureDependenciesExistsOnAllNodes(extensionAddress);

	MarkObjectDistributed(extensionAddress);
}


List *
PlanDropExtensionStmt(DropStmt *stmt, const char *queryString)
{
	List *commands = NIL;
	ListCell *addressCell = NULL;
	List *oldTypes = stmt->objects;
	List *distributedExtensions = NIL;
	List *distributedExtensionAddresses = NIL;
	const char *deparsedStmt = NULL;

	if (!ShouldPropagateCreateDropExtension())
	{
		return NIL;
	}

	distributedExtensions = FilterNameListForDistributedExtensions(oldTypes);

	if (list_length(distributedExtensions) <= 0)
	{
		/* no distributed extensions to drop */
		return NIL;
	}

	EnsureCoordinator();

	distributedExtensionAddresses = ExtensionNameListToObjectAddresses(
		distributedExtensions);

	foreach(addressCell, distributedExtensionAddresses)
	{
		ObjectAddress *address = (ObjectAddress *) lfirst(addressCell);
		UnmarkObjectDistributed(address);
	}

	/*
	 * temporary swap the lists of objects to delete with the distributed objects and
	 * deparse to an executable sql statement for the workers
	 */
	stmt->objects = distributedExtensions;
	deparsedStmt = DeparseTreeNode((Node *) stmt);
	stmt->objects = oldTypes;

	commands = list_make3(DISABLE_DDL_PROPAGATION,
						  (void *) deparsedStmt,
						  ENABLE_DDL_PROPAGATION);

	return NodeDDLTaskList(ALL_WORKERS, commands);
}


static List *
FilterNameListForDistributedExtensions(List *objects)
{
	ListCell *objectCell = NULL;
	List *result = NIL;

	bool missingOk = true;

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


static List *
ExtensionNameListToObjectAddresses(List *objects)
{
	ListCell *objectCell = NULL;
	List *result = NIL;
	bool missingOk = true;

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


static bool
ShouldPropagateCreateDropExtension(void)
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
 * Add missing fields to CreateExtensionStmt before deparsing it.
 */
static void
AddMissingFieldsCreateExtensionStmt(CreateExtensionStmt *stmt)
{
	List *optionsList = stmt->options;

	const char *newVersion = GetCreateExtensionOption(optionsList, "new_version");
	const char *schemaName = GetCreateExtensionOption(optionsList, "schema");

	if (!newVersion)
	{
		/* TODO: find the latest version available version of the extension in coordinator and append it instead of "version_num" */

		char *extensionName = stmt->extname;
		char *extensionDefaultVersion = GetDefaultExtensionVersion(extensionName);
		DefElem *newDefElement = makeDefElem("new_version", (Node *) (makeString(
																		  extensionDefaultVersion)),
											 -1);
		stmt->options = lappend(stmt->options, newDefElement);
	}
	if (!schemaName)
	{
		char *currentSchemaName = GetCurrentSchema();

		DefElem *newDefElement = makeDefElem("schema", (Node *) (makeString(
																	 currentSchemaName)),
											 -1);
		stmt->options = lappend(stmt->options, newDefElement);
	}
}


const char *
GetCreateExtensionOption(List *defElemOptions, const char *optionName)
{
	const char *targetArg = NULL;

	ListCell *optionsCell = NULL;

	foreach(optionsCell, defElemOptions)
	{
		DefElem *defElement = (DefElem *) lfirst(optionsCell);

		if (IsA(defElement, DefElem) && strncmp(defElement->defname, optionName,
												NAMEDATALEN) == 0)
		{
			targetArg = strVal(defElement->arg);
			break;
		}
	}

	return targetArg;
}


/*
 * TODO: add comments: Cannot return NULL, errors out as Postgres would do if NULL
 *
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
 * TODO: add comment
 *
 *
 * Cannot return NULL, it'd error out as Postgres does if the version is not avalible
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
