/*-------------------------------------------------------------------------
 *
 * grant.c
 *    Declarations for public functions and variables use for granting
 *    access to distributed tables.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_GRANT_H
#define CITUS_GRANT_H

#include "nodes/parsenodes.h"

extern List * PlanGrantStmt(GrantStmt *grantStmt);

#endif /*CITUS_GRANT_H */
