/*-------------------------------------------------------------------------
 *
 * tablecmds.h
 *    Declarations for public functions and variables use for altering and
 *    creating distributed tables.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_TABLECMDS_H
#define CITUS_TABLECMDS_H

#include "c.h"

#include "nodes/parsenodes.h"

extern void ProcessDropTableStmt(DropStmt *dropTableStatement);
extern void ProcessTruncateStatement(TruncateStmt *truncateStatement);
extern void ProcessCreateTableStmtPartitionOf(CreateStmt *createStatement);
extern void ProcessAlterTableStmtAttachPartition(AlterTableStmt *alterTableStatement);
extern List * PlanAlterTableStmt(AlterTableStmt *alterTableStatement,
								 const char *alterTableCommand);
extern Node * WorkerProcessAlterTableStmt(AlterTableStmt *alterTableStatement,
										  const char *alterTableCommand);
extern bool IsAlterTableRenameStmt(RenameStmt *renameStmt);
extern void ErrorIfAlterDropsPartitionColumn(AlterTableStmt *alterTableStatement);
extern void ErrorUnsupportedAlterTableAddColumn(Oid relationId, AlterTableCmd *command,
												Constraint *constraint);

#endif /*CITUS_TABLECMDS_H */
