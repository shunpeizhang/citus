/*-------------------------------------------------------------------------
 *
 * schemacmds.h
 *    Declarations for public functions and variables use for altering and
 *    creating schemas for distributed tables.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_SCHEMACMDS_H
#define CITUS_SCHEMACMDS_H

#include "nodes/parsenodes.h"

extern void ProcessDropSchemaStmt(DropStmt *dropSchemaStatement);
extern List * PlanAlterObjectSchemaStmt(AlterObjectSchemaStmt *alterObjectSchemaStmt,
										const char *alterObjectSchemaCommand);

#endif /*CITUS_SCHEMACMDS_H */
