
#include "postgres.h"

#include "distributed/deparser.h"
#include "lib/stringinfo.h"
#include "nodes/pg_list.h"
#include "nodes/parsenodes.h"
#include "utils/builtins.h"

static void AppendCreateExtensionStmt(StringInfo str, CreateExtensionStmt *stmt);
static void AppendDropExtensionStmt(StringInfo buf, DropStmt *stmt);
static void AppendExtensionNameList(StringInfo buf, List *objects);

const char *
DeparseCreateExtensionStmt(CreateExtensionStmt *stmt)
{
	StringInfoData sql = { 0 };
	initStringInfo(&sql);

	AppendCreateExtensionStmt(&sql, stmt);

	return sql.data;
}


static void
AppendCreateExtensionStmt(StringInfo str, CreateExtensionStmt *stmt)
{
	/*
	 * Read required fields from parsed stmt
	 * We do not fetch if_not_exist and cascade fields as we will add them by default
	 * We as also do not care old_version for now
	 */
	const char *extensionName = stmt->extname;

	List *optionsList = stmt->options;

	const char *newVersion = quote_identifier(GetCreateExtensionOption(optionsList,
																	   "new_version"));
	const char *schemaName = GetCreateExtensionOption(optionsList, "schema");

	appendStringInfo(str,
					 "CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s VERSION %s CASCADE",
					 extensionName, schemaName, newVersion);
}


/*
 * DeparseDropExtensionStmt builds and returns a string representing the DropStmt
 */
const char *
DeparseDropExtensionStmt(DropStmt *stmt)
{
	StringInfoData str = { 0 };
	initStringInfo(&str);

	AppendDropExtensionStmt(&str, stmt);

	return str.data;
}


/*
 * AppendDropExtensionStmt appends a string representing the DropStmt to a buffer
 */
static void
AppendDropExtensionStmt(StringInfo buf, DropStmt *stmt)
{
	appendStringInfoString(buf, "DROP EXTENSION IF EXISTS ");

	AppendExtensionNameList(buf, stmt->objects);

	appendStringInfoString(buf, " CASCADE;");
}


/*
 * AppendExtensionNameList appends a string representing the list of extension names to a buffer
 */
static void
AppendExtensionNameList(StringInfo buf, List *objects)
{
	ListCell *objectCell = NULL;
	foreach(objectCell, objects)
	{
		const char *extensionName = strVal(lfirst(objectCell));

		if (objectCell != list_head(objects))
		{
			appendStringInfo(buf, ", ");
		}

		appendStringInfoString(buf, extensionName);
	}
}
