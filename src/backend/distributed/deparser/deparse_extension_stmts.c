/*-------------------------------------------------------------------------
 *
 * deparse_extension_stmts.c
 *	  All routines to deparse extension statements.
 *	  This file contains deparse functions for extension statement deparsing
 *    as well as related helper functions.
 *
 * Copyright (c), Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "catalog/namespace.h"
#include "distributed/deparser.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"

/* Local functions forward declarations for helper functions */
static void AppendCreateExtensionStmt(StringInfo buf, CreateExtensionStmt *stmt);
static void AppendDropExtensionStmt(StringInfo buf, DropStmt *stmt);
static void AppendExtensionNameList(StringInfo buf, List *objects);
static void AppendAlterExtensionSchemaStmt(StringInfo buf,
										   AlterObjectSchemaStmt *alterExtensionSchemaStmt);
static void AppendAlterExtensionStmt(StringInfo buf,
									 AlterExtensionStmt *alterExtensionStmt);


/*
 * GetCreateExtensionOption fetches the string value of DefElem node with
 * "defname" from "options" list
 */
const char *
GetCreateAlterExtensionOption(List *extensionOptions, const char *defname)
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
 * DeparseCreateExtensionStmt builds and returns a string representing the
 * CreateExtensionStmt to be sent to worker nodes.
 */
const char *
DeparseCreateExtensionStmt(CreateExtensionStmt *createExtensionStmt)
{
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	AppendCreateExtensionStmt(&sql, createExtensionStmt);

	return sql.data;
}


/*
 * AppendCreateExtensionStmt appends a string representing the CreateExtensionStmt to a buffer
 */
static void
AppendCreateExtensionStmt(StringInfo buf, CreateExtensionStmt *createExtensionStmt)
{
	const char *extensionName = createExtensionStmt->extname;

	List *optionsList = createExtensionStmt->options;

	/*
	 * We do only fetch "new_version" and "schema" options from optionList
	 * as we will append "IF NOT EXISTS" and "CASCADE" clauses regardless
	 * of statement's content before propagating it to worker nodes.
	 * We also do not care old_version for now.
	 */
	const char *newVersion = GetCreateAlterExtensionOption(optionsList, "new_version");
	const char *schemaName = GetCreateAlterExtensionOption(optionsList, "schema");

	schemaName = quote_identifier(schemaName);

	appendStringInfo(buf, "CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s",
					 extensionName, schemaName);

	/* "new_version" may not be specified in CreateExtensionStmt */
	if (newVersion)
	{
		newVersion = quote_identifier(newVersion);

		appendStringInfo(buf, " VERSION %s", newVersion);
	}

	appendStringInfoString(buf, " CASCADE;");
}


/*
 * DeparseAlterExtensionStmt builds and returns a string representing the
 * AlterExtensionStmt to be sent to worker nodes.
 */
const char *
DeparseAlterExtensionStmt(AlterExtensionStmt *alterExtensionStmt)
{
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	AppendAlterExtensionStmt(&sql, alterExtensionStmt);

	return sql.data;
}


/*
 * AppendAlterExtensionStmt appends a string representing the AlterExtensionStmt to a buffer
 */
static void
AppendAlterExtensionStmt(StringInfo buf, AlterExtensionStmt *alterExtensionStmt)
{
	const char *extensionName = alterExtensionStmt->extname;

	List *optionsList = alterExtensionStmt->options;

	const char *newVersion = GetCreateAlterExtensionOption(optionsList, "new_version");

	appendStringInfo(buf, "ALTER EXTENSION %s UPDATE ", extensionName);

	/* "new_version" may not be specified in AlterExtensionStmt */
	if (newVersion)
	{
		newVersion = quote_identifier(newVersion);

		appendStringInfo(buf, " TO %s", newVersion);
	}

	appendStringInfoString(buf, ";");
}


/*
 * DeparseDropExtensionStmt builds and returns a string representing the DropStmt
 */
const char *
DeparseDropExtensionStmt(DropStmt *dropStmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendDropExtensionStmt(&str, dropStmt);

	return str.data;
}


/*
 * AppendDropExtensionStmt appends a string representing the DropStmt for
 * an extension to a buffer.
 */
static void
AppendDropExtensionStmt(StringInfo str, DropStmt *dropStmt)
{
	/* we append "IF NOT EXISTS" clause regardless of the content of the statement. */
	appendStringInfoString(str, "DROP EXTENSION IF EXISTS ");

	/*
	 * Pick the distributed ones from the  "objects" list that is storing
	 * the object names to be deleted.
	 */
	AppendExtensionNameList(str, dropStmt->objects);

	/* depending on behaviour field of DropStmt, we should append CASCADE or RESTRICT */
	if (dropStmt->behavior == DROP_CASCADE)
	{
		appendStringInfoString(str, " CASCADE;");
	}
	else
	{
		appendStringInfoString(str, " RESTRICT;");
	}
}


/*
 * AppendExtensionNameList appends a string representing the list of
 * extension names to a buffer.
 */
static void
AppendExtensionNameList(StringInfo str, List *objects)
{
	ListCell *objectCell = NULL;
	foreach(objectCell, objects)
	{
		const char *extensionName = strVal(lfirst(objectCell));

		if (objectCell != list_head(objects))
		{
			appendStringInfo(str, ", ");
		}

		appendStringInfoString(str, extensionName);
	}
}


/*
 * DeparseAlterExtensionSchemaStmt builds and returns a string representing the
 * AlterObjectSchemaStmt (ALTER EXTENSION SET SCHEMA).
 */
const char *
DeparseAlterExtensionSchemaStmt(AlterObjectSchemaStmt *alterExtensionSchemaStmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	Assert(alterExtensionSchemaStmt->objectType == OBJECT_EXTENSION);

	AppendAlterExtensionSchemaStmt(&str, alterExtensionSchemaStmt);

	return str.data;
}


/*
 * AppendAlterExtensionSchemaStmt appends a string representing the AlterObjectSchemaStmt
 * for an extension to a buffer.
 */
static void
AppendAlterExtensionSchemaStmt(StringInfo buf,
							   AlterObjectSchemaStmt *alterExtensionSchemaStmt)
{
	const char *extensionName = NULL;

	Assert(alterExtensionSchemaStmt->objectType == OBJECT_EXTENSION);

	extensionName = strVal(alterExtensionSchemaStmt->object);
	appendStringInfo(buf, "ALTER EXTENSION %s SET SCHEMA %s;", extensionName,
					 quote_identifier(alterExtensionSchemaStmt->newschema));
}
