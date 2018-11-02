/*-------------------------------------------------------------------------
 *
 * utility_hook.h
 *	  Citus utility hook and related functionality.
 *
 * Copyright (c) 2012-2016, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#ifndef MULTI_UTILITY_H
#define MULTI_UTILITY_H

#include "postgres.h"

#include "utils/relcache.h"
#include "tcop/utility.h"

extern bool EnableDDLPropagation;


#if (PG_VERSION_NUM < 100000)
struct QueryEnvironment; /* forward-declare to appease compiler */
#endif

extern void multi_ProcessUtility(PlannedStmt *pstmt, const char *queryString,
								 ProcessUtilityContext context, ParamListInfo params,
								 struct QueryEnvironment *queryEnv, DestReceiver *dest,
								 char *completionTag);
extern void multi_ProcessUtility9x(Node *parsetree, const char *queryString,
								   ProcessUtilityContext context, ParamListInfo params,
								   DestReceiver *dest, char *completionTag);
extern void CitusProcessUtility(Node *node, const char *queryString,
								ProcessUtilityContext context, ParamListInfo params,
								DestReceiver *dest, char *completionTag);

#endif /* MULTI_UTILITY_H */
