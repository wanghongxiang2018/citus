/*-------------------------------------------------------------------------
 *
 * extended_op_node_utils.c implements the logic for building the necessary
 * information that is shared among both the worker and master extended
 * op nodes.
 *
 * Copyright (c) Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "distributed/extended_op_node_utils.h"
#include "distributed/metadata_cache.h"
#include "distributed/multi_logical_optimizer.h"
#include "distributed/pg_dist_partition.h"
#if PG_VERSION_NUM >= 120000
#include "optimizer/optimizer.h"
#else
#include "optimizer/var.h"
#endif
#include "optimizer/restrictinfo.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"


static bool GroupedByDisjointPartitionColumn(List *tableNodeList,
											 MultiExtendedOp *opNode);


/*
 * BuildExtendedOpNodeProperties is a helper function that simply builds
 * the necessary information for processing the extended op node. The return
 * value should be used in a read-only manner.
 */
ExtendedOpNodeProperties
BuildExtendedOpNodeProperties(MultiExtendedOp *extendedOpNode, bool
							  pullUpIntermediateRows)
{
	ExtendedOpNodeProperties extendedOpNodeProperties;


	List *tableNodeList = FindNodesOfType((MultiNode *) extendedOpNode, T_MultiTable);
	bool groupedByDisjointPartitionColumn = GroupedByDisjointPartitionColumn(
		tableNodeList,
		extendedOpNode);

	/*
	 * TODO: Only window functions that can be pushed down reach here, thus,
	 * using hasWindowFuncs is safe for now. However, this should be fixed
	 * when we support pull-to-master window functions.
	 */
	bool pushDownWindowFunctions = extendedOpNode->hasWindowFuncs;

	extendedOpNodeProperties.groupedByDisjointPartitionColumn =
		groupedByDisjointPartitionColumn;
	extendedOpNodeProperties.pushDownWindowFunctions = pushDownWindowFunctions;
	extendedOpNodeProperties.pullUpIntermediateRows = pullUpIntermediateRows;

	return extendedOpNodeProperties;
}


/*
 * GroupedByDisjointPartitionColumn returns true if the query is grouped by the
 * partition column of a table whose shards have disjoint sets of partition values.
 */
static bool
GroupedByDisjointPartitionColumn(List *tableNodeList, MultiExtendedOp *opNode)
{
	bool result = false;
	ListCell *tableNodeCell = NULL;

	foreach(tableNodeCell, tableNodeList)
	{
		MultiTable *tableNode = (MultiTable *) lfirst(tableNodeCell);
		Oid relationId = tableNode->relationId;

		if (relationId == SUBQUERY_RELATION_ID || !IsDistributedTable(relationId))
		{
			continue;
		}

		char partitionMethod = PartitionMethod(relationId);
		if (partitionMethod != DISTRIBUTE_BY_RANGE &&
			partitionMethod != DISTRIBUTE_BY_HASH)
		{
			continue;
		}

		if (GroupedByColumn(opNode->groupClauseList, opNode->targetList,
							tableNode->partitionColumn))
		{
			result = true;
			break;
		}
	}

	return result;
}
