/*-------------------------------------------------------------------------
 *
 * indexcmds.h
 *    Declarations for public functions and variables use for altering and
 *    creating indices on distributed tables.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_INDEXCMDS_H
#define CITUS_INDEXCMDS_H

#include "nodes/parsenodes.h"

extern List * PlanDropIndexStmt(DropStmt *dropIndexStatement,
								const char *dropIndexCommand);

#endif /*CITUS_INDEXCMDS_H */
