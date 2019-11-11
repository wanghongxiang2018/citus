
#include "postgres.h"

#include "distributed/deparser.h"
#include "lib/stringinfo.h"
#include "parser/parse_type.h"
#include "server/nodes/pg_list.h"
#include "server/nodes/value.h"
#include "server/utils/builtins.h"

static void AppendCreateExtensionStmt(StringInfo str, CreateExtensionStmt *stmt);

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
    CreateExtensionOptions fetchedOptions;
    const char *new_version = NULL, *schema = NULL;

    fetchedOptions = FetchCreateExtensionOptionList(stmt);
    
    // TODO: quote_identifier of quote_qualified_identifier ??
    new_version = quote_identifier(fetchedOptions.new_version);
    schema = fetchedOptions.schemaName;

	appendStringInfo(str, "CREATE EXTENSION IF NOT EXISTS %s WITH SCHEMA %s VERSION %s CASCADE",
        extensionName, schema, new_version);

    elog(DEBUG1, str->data);
}

CreateExtensionOptions
FetchCreateExtensionOptionList(CreateExtensionStmt *stmt) 
{
	CreateExtensionOptions fetchedOptions = { 0 };

	List *optionsList = stmt->options;
	ListCell *optionsCell = NULL;
	
	foreach(optionsCell, optionsList)
	{
		DefElem *defElement = (DefElem *) lfirst(optionsCell);

		if (strncmp(defElement->defname, "new_version", NAMEDATALEN) == 0)
		{
			fetchedOptions.new_version = strVal(defElement->arg);
		}
		else if (strncmp(defElement->defname, "schema", NAMEDATALEN) == 0)
		{
			fetchedOptions.schemaName = strVal(defElement->arg);
		}
	}

	return fetchedOptions;
}