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

#include "nodes/parsenodes.h"

extern void ProcessTruncateStatement(TruncateStmt *truncateStatement);

#endif /*CITUS_TABLECMDS_H */
