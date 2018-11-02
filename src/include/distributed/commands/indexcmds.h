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

#include "c.h"

#include "nodes/parsenodes.h"

extern bool IsIndexRenameStmt(RenameStmt *renameStmt);
extern List * PlanIndexStmt(IndexStmt *createIndexStatement,
							const char *createIndexCommand);
extern List * PlanDropIndexStmt(DropStmt *dropIndexStatement,
								const char *dropIndexCommand);
extern void PostProcessIndexStmt(IndexStmt *indexStmt);

#endif /*CITUS_INDEXCMDS_H */
