/*-------------------------------------------------------------------------
 *
 * common.c
 *    Common functions shard by multiple commands
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_COMMON_H
#define CITUS_COMMON_H

#include "postgres.h"

#include "nodes/parsenodes.h"


/*
 * A DDLJob encapsulates the remote tasks and commands needed to process all or
 * part of a distributed DDL command. It hold the distributed relation's oid,
 * the original DDL command string (for MX DDL propagation), and a task list of
 * DDL_TASK-type Tasks to be executed.
 */
typedef struct DDLJob
{
	Oid targetRelationId;      /* oid of the target distributed relation */
	bool concurrentIndexCmd;   /* related to a CONCURRENTLY index command? */
	bool executeSequentially;
	const char *commandString; /* initial (coordinator) DDL command string */
	List *taskList;            /* worker DDL tasks to execute */
} DDLJob;


extern void MarkInvalidateForeignKeyGraph(void);
extern void InvalidateForeignKeyGraphForDDL(void);
extern List * DDLTaskList(Oid relationId, const char *commandString);

#endif /*CITUS_COMMON_H */
