/*-------------------------------------------------------------------------
 *
 * sequence.h
 *    Declarations for public functions and variables used in CREATE and
 *    ALTER SEQUENCE statments
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_SEQUENCE_H
#define CITUS_SEQUENCE_H

#include "nodes/parsenodes.h"

extern void ErrorIfUnsupportedSeqStmt(CreateSeqStmt *createSeqStmt);
extern void ErrorIfDistributedAlterSeqOwnedBy(AlterSeqStmt *alterSeqStmt);

#endif /*CITUS_SEQUENCE_H */
