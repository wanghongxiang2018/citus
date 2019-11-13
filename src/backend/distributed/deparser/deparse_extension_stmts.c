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

#include "distributed/deparser.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"

/* Local functions forward declarations for helper functions */
static void AppendCreateExtensionStmt(StringInfo str, CreateExtensionStmt *stmt);
static void AppendDropExtensionStmt(StringInfo buf, DropStmt *stmt);
static void AppendExtensionNameList(StringInfo buf, List *objects);


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
AppendCreateExtensionStmt(StringInfo str, CreateExtensionStmt *createExtensionStmt)
{
	/*
	 * Read required options from createExtensionStmt.
	 * We do not fetch if_not_exist and cascade options as we will
	 * append "IF NOT EXISTS" and "CASCADE" clauses regardless of
	 * statement's content before propagating it to worker nodes.
	 * We as also do not care old_version for now.
	 */
	const char *extensionName = createExtensionStmt->extname;

	List *optionsList = createExtensionStmt->options;

	const char *newVersion = GetCreateExtensionOption(optionsList, "new_version");
	const char *schemaName = GetCreateExtensionOption(optionsList, "schema");

	newVersion = quote_identifier(newVersion);
	schemaName = quote_identifier(schemaName);

	appendStringInfo(str,
					 "CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s VERSION %s CASCADE",
					 extensionName, schemaName, newVersion);
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
 * AppendDropExtensionStmt appends a string representing the DropStmt for an extension to a buffer.
 */
static void
AppendDropExtensionStmt(StringInfo str, DropStmt *dropStmt)
{
	/*
	 * We do not fetch "missing_ok" and "behaviour" fields as we will
	 * append "CASCADE" and "IF NOT EXISTS" clauses regardless of
	 * the content of the statement.
	 * Here we only need to fetch "objects" list that is storing the
	 * object names to be deleted.
	 */
	appendStringInfoString(str, "DROP EXTENSION IF EXISTS ");

	AppendExtensionNameList(str, dropStmt->objects);

	appendStringInfoString(str, " CASCADE;");
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
