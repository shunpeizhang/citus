/*-------------------------------------------------------------------------
 * utility_hook.c
 *	  Citus utility hook and related functionality.
 *
 * The utility hook is called by PostgreSQL when processing any command
 * that is not SELECT, UPDATE, DELETE, INSERT, in place of the regular
 * ProcessUtility function. We use this primarily to implement (or in
 * some cases prevent) DDL commands and COPY on distributed tables.
 *
 * For DDL commands that affect distributed tables, we check whether
 * they are valid (and implemented) for the distributed table and then
 * propagate the command to all shards and, in case of MX, to distributed
 * tables on other nodes. We still call the original ProcessUtility
 * function to apply catalog changes on the coordinator.
 *
 * For COPY into a distributed table, we provide an alternative
 * implementation in ProcessCopyStmt that sends rows to shards based
 * on their distribution column value instead of writing it to the local
 * table on the coordinator. For COPY from a distributed table, we
 * replace the table with a SELECT * FROM table and pass it back to
 * ProcessUtility, which will plan the query via the distributed planner
 * hook.
 *
 * Copyright (c) 2012-2018, Citus Data, Inc.
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "c.h"
#include "libpq-fe.h"
#include "miscadmin.h"
#include "port.h"

#include <string.h>

#include "access/attnum.h"
#include "access/heapam.h"
#include "access/htup.h"
#include "access/htup_details.h"
#include "access/tupdesc.h"
#include "access/xact.h"
#include "catalog/catalog.h"
#include "catalog/dependency.h"
#include "catalog/index.h"
#include "catalog/indexing.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "citus_version.h"
#include "commands/dbcommands.h"
#include "commands/defrem.h"
#include "commands/tablecmds.h"
#include "commands/prepare.h"
#include "distributed/citus_ruleutils.h"
#include "distributed/colocation_utils.h"
#include "distributed/commands/common.h"
#include "distributed/commands/indexcmds.h"
#include "distributed/commands/schemacmds.h"
#include "distributed/commands/sequence.h"
#include "distributed/commands/tablecmds.h"
#include "distributed/commands/vacuum.h"
#include "distributed/foreign_constraint.h"
#include "distributed/intermediate_results.h"
#include "distributed/listutils.h"
#include "distributed/maintenanced.h"
#include "distributed/master_metadata_utility.h"
#include "distributed/master_protocol.h"
#include "distributed/metadata_cache.h"
#include "distributed/metadata_sync.h"
#include "distributed/multi_copy.h"
#include "distributed/multi_partitioning_utils.h"
#include "distributed/distributed_planner.h"
#include "distributed/multi_router_executor.h"
#include "distributed/multi_router_planner.h"
#include "distributed/multi_shard_transaction.h"
#include "distributed/utility_hook.h" /* IWYU pragma: keep */
#include "distributed/pg_dist_partition.h"
#include "distributed/policy.h"
#include "distributed/reference_table_utils.h"
#include "distributed/relation_access_tracking.h"
#include "distributed/resource_lock.h"
#include "distributed/transaction_management.h"
#include "distributed/transmit.h"
#include "distributed/worker_transaction.h"
#include "distributed/version_compat.h"
#include "executor/executor.h"
#include "lib/stringinfo.h"
#include "nodes/bitmapset.h"
#include "nodes/memnodes.h"
#include "nodes/nodes.h"
#include "nodes/params.h"
#include "nodes/parsenodes.h"
#include "nodes/pg_list.h"
#include "nodes/primnodes.h"
#include "nodes/value.h"
#include "parser/analyze.h"
#include "storage/lmgr.h"
#include "storage/lock.h"
#include "tcop/dest.h"
#include "tcop/utility.h"
#include "utils/acl.h"
#include "utils/builtins.h"
#include "utils/elog.h"
#include "utils/errcodes.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/hsearch.h"
#include "utils/inval.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/palloc.h"
#include "utils/syscache.h"


bool EnableDDLPropagation = true; /* ddl propagation is enabled */


/* Local functions forward declarations for deciding when to perform processing/checks */
static bool IsCitusExtensionStmt(Node *parsetree);

/* Local functions forward declarations for processing distributed table commands */
static void ProcessCreateTableStmtPartitionOf(CreateStmt *createStatement);
static void ProcessAlterTableStmtAttachPartition(AlterTableStmt *alterTableStatement);
static List * PlanRenameStmt(RenameStmt *renameStmt, const char *renameCommand);
static List * PlanAlterObjectSchemaStmt(AlterObjectSchemaStmt *alterObjectSchemaStmt,
										const char *alterObjectSchemaCommand);


/* Local functions forward declarations for unsupported command checks */
static void ErrorIfUnstableCreateOrAlterExtensionStmt(Node *parsetree);
static void ErrorIfUnsupportedRenameStmt(RenameStmt *renameStmt);
static void ErrorIfUnsupportedAlterAddConstraintStmt(AlterTableStmt *alterTableStatement);

/* Local functions forward declarations for helper functions */
static char * ExtractNewExtensionVersion(Node *parsetree);
static bool IsAlterTableRenameStmt(RenameStmt *renameStmt);
static bool IsIndexRenameStmt(RenameStmt *renameStmt);
static void ExecuteDistributedDDLJob(DDLJob *ddlJob);
static char * SetSearchPathToCurrentSearchPathCommand(void);
static char * CurrentSearchPath(void);
static void PostProcessUtility(Node *parsetree);
extern List * PlanGrantStmt(GrantStmt *grantStmt);
static List * CollectGrantTableIdList(GrantStmt *grantStmt);

static void ErrorUnsupportedAlterTableAddColumn(Oid relationId, AlterTableCmd *command,
												Constraint *constraint);


/*
 * multi_ProcessUtility9x is the 9.x-compatible wrapper for Citus' main utility
 * hook. It simply adapts the old-style hook to call into the new-style (10+)
 * hook, which is what now houses all actual logic.
 */
void
multi_ProcessUtility9x(Node *parsetree,
					   const char *queryString,
					   ProcessUtilityContext context,
					   ParamListInfo params,
					   DestReceiver *dest,
					   char *completionTag)
{
	PlannedStmt *plannedStmt = makeNode(PlannedStmt);
	plannedStmt->commandType = CMD_UTILITY;
	plannedStmt->utilityStmt = parsetree;

	multi_ProcessUtility(plannedStmt, queryString, context, params, NULL, dest,
						 completionTag);
}


/*
 * CitusProcessUtility is a version-aware wrapper of ProcessUtility to account
 * for argument differences between the 9.x and 10+ PostgreSQL versions.
 */
void
CitusProcessUtility(Node *node, const char *queryString, ProcessUtilityContext context,
					ParamListInfo params, DestReceiver *dest, char *completionTag)
{
#if (PG_VERSION_NUM >= 100000)
	PlannedStmt *plannedStmt = makeNode(PlannedStmt);
	plannedStmt->commandType = CMD_UTILITY;
	plannedStmt->utilityStmt = node;

	ProcessUtility(plannedStmt, queryString, context, params, NULL, dest,
				   completionTag);
#else
	ProcessUtility(node, queryString, context, params, dest, completionTag);
#endif
}


/*
 * multi_ProcessUtility is the main entry hook for implementing Citus-specific
 * utility behavior. Its primary responsibilities are intercepting COPY and DDL
 * commands and augmenting the coordinator's command with corresponding tasks
 * to be run on worker nodes, after suitably ensuring said commands' options
 * are fully supported by Citus. Much of the DDL behavior is toggled by Citus'
 * enable_ddl_propagation GUC. In addition to DDL and COPY, utilities such as
 * TRUNCATE and VACUUM are also supported.
 */
void
multi_ProcessUtility(PlannedStmt *pstmt,
					 const char *queryString,
					 ProcessUtilityContext context,
					 ParamListInfo params,
					 struct QueryEnvironment *queryEnv,
					 DestReceiver *dest,
					 char *completionTag)
{
	Node *parsetree = pstmt->utilityStmt;
	bool commandMustRunAsOwner = false;
	Oid savedUserId = InvalidOid;
	int savedSecurityContext = 0;
	List *ddlJobs = NIL;
	bool checkExtensionVersion = false;

	if (IsA(parsetree, TransactionStmt))
	{
		/*
		 * Transaction statements (e.g. ABORT, COMMIT) can be run in aborted
		 * transactions in which case a lot of checks cannot be done safely in
		 * that state. Since we never need to intercept transaction statements,
		 * skip our checks and immediately fall into standard_ProcessUtility.
		 */
#if (PG_VERSION_NUM >= 100000)
		standard_ProcessUtility(pstmt, queryString, context,
								params, queryEnv, dest, completionTag);
#else
		standard_ProcessUtility(parsetree, queryString, context,
								params, dest, completionTag);
#endif

		return;
	}

	checkExtensionVersion = IsCitusExtensionStmt(parsetree);
	if (EnableVersionChecks && checkExtensionVersion)
	{
		ErrorIfUnstableCreateOrAlterExtensionStmt(parsetree);
	}


	if (!CitusHasBeenLoaded())
	{
		/*
		 * Ensure that utility commands do not behave any differently until CREATE
		 * EXTENSION is invoked.
		 */
#if (PG_VERSION_NUM >= 100000)
		standard_ProcessUtility(pstmt, queryString, context,
								params, queryEnv, dest, completionTag);
#else
		standard_ProcessUtility(parsetree, queryString, context,
								params, dest, completionTag);
#endif

		return;
	}

#if (PG_VERSION_NUM >= 110000)
	if (IsA(parsetree, CallStmt))
	{
		/*
		 * Stored procedures are a bit strange in the sense that some statements
		 * are not in a transaction block, but can be rolled back. We need to
		 * make sure we send all statements in a transaction block. The
		 * StoredProcedureLevel variable signals this to the router executor
		 * and indicates how deep in the call stack we are in case of nested
		 * stored procedures.
		 */
		StoredProcedureLevel += 1;

		PG_TRY();
		{
			standard_ProcessUtility(pstmt, queryString, context,
									params, queryEnv, dest, completionTag);

			StoredProcedureLevel -= 1;
		}
		PG_CATCH();
		{
			StoredProcedureLevel -= 1;
			PG_RE_THROW();
		}
		PG_END_TRY();

		return;
	}
#endif

	/*
	 * TRANSMIT used to be separate command, but to avoid patching the grammar
	 * it's no overlaid onto COPY, but with FORMAT = 'transmit' instead of the
	 * normal FORMAT options.
	 */
	if (IsTransmitStmt(parsetree))
	{
		CopyStmt *copyStatement = (CopyStmt *) parsetree;

		VerifyTransmitStmt(copyStatement);

		/* ->relation->relname is the target file in our overloaded COPY */
		if (copyStatement->is_from)
		{
			RedirectCopyDataToRegularFile(copyStatement->relation->relname);
		}
		else
		{
			SendRegularFile(copyStatement->relation->relname);
		}

		/* Don't execute the faux copy statement */
		return;
	}

	if (IsA(parsetree, CopyStmt))
	{
		MemoryContext planContext = GetMemoryChunkContext(parsetree);
		MemoryContext previousContext;

		parsetree = copyObject(parsetree);
		parsetree = ProcessCopyStmt((CopyStmt *) parsetree, completionTag,
									&commandMustRunAsOwner);

		previousContext = MemoryContextSwitchTo(planContext);
		parsetree = copyObject(parsetree);
		MemoryContextSwitchTo(previousContext);

		if (parsetree == NULL)
		{
			return;
		}
	}

	/* we're mostly in DDL (and VACUUM/TRUNCATE) territory at this point... */

	if (IsA(parsetree, CreateSeqStmt))
	{
		ErrorIfUnsupportedSeqStmt((CreateSeqStmt *) parsetree);
	}

	if (IsA(parsetree, AlterSeqStmt))
	{
		ErrorIfDistributedAlterSeqOwnedBy((AlterSeqStmt *) parsetree);
	}

	if (IsA(parsetree, TruncateStmt))
	{
		ProcessTruncateStatement((TruncateStmt *) parsetree);
	}

	/* only generate worker DDLJobs if propagation is enabled */
	if (EnableDDLPropagation)
	{
		if (IsA(parsetree, IndexStmt))
		{
			MemoryContext oldContext = MemoryContextSwitchTo(GetMemoryChunkContext(
																 parsetree));

			/* copy parse tree since we might scribble on it to fix the schema name */
			parsetree = copyObject(parsetree);

			MemoryContextSwitchTo(oldContext);

			ddlJobs = PlanIndexStmt((IndexStmt *) parsetree, queryString);
		}

		if (IsA(parsetree, DropStmt))
		{
			DropStmt *dropStatement = (DropStmt *) parsetree;
			if (dropStatement->removeType == OBJECT_INDEX)
			{
				ddlJobs = PlanDropIndexStmt(dropStatement, queryString);
			}

			if (dropStatement->removeType == OBJECT_TABLE)
			{
				ProcessDropTableStmt(dropStatement);
			}

			if (dropStatement->removeType == OBJECT_SCHEMA)
			{
				ProcessDropSchemaStmt(dropStatement);
			}

			if (dropStatement->removeType == OBJECT_POLICY)
			{
				ddlJobs = PlanDropPolicyStmt(dropStatement, queryString);
			}
		}

		if (IsA(parsetree, AlterTableStmt))
		{
			AlterTableStmt *alterTableStmt = (AlterTableStmt *) parsetree;
			if (alterTableStmt->relkind == OBJECT_TABLE ||
				alterTableStmt->relkind == OBJECT_INDEX)
			{
				ddlJobs = PlanAlterTableStmt(alterTableStmt, queryString);
			}
		}

		/*
		 * ALTER TABLE ... RENAME statements have their node type as RenameStmt and
		 * not AlterTableStmt. So, we intercept RenameStmt to tackle these commands.
		 */
		if (IsA(parsetree, RenameStmt))
		{
			ddlJobs = PlanRenameStmt((RenameStmt *) parsetree, queryString);
		}

		/*
		 * ALTER ... SET SCHEMA statements have their node type as AlterObjectSchemaStmt.
		 * So, we intercept AlterObjectSchemaStmt to tackle these commands.
		 */
		if (IsA(parsetree, AlterObjectSchemaStmt))
		{
			AlterObjectSchemaStmt *setSchemaStmt = (AlterObjectSchemaStmt *) parsetree;
			ddlJobs = PlanAlterObjectSchemaStmt(setSchemaStmt, queryString);
		}

		if (IsA(parsetree, CreatePolicyStmt))
		{
			ddlJobs = PlanCreatePolicyStmt((CreatePolicyStmt *) parsetree);
		}

		if (IsA(parsetree, AlterPolicyStmt))
		{
			ddlJobs = PlanAlterPolicyStmt((AlterPolicyStmt *) parsetree);
		}

		/*
		 * ALTER TABLE ALL IN TABLESPACE statements have their node type as
		 * AlterTableMoveAllStmt. At the moment we do not support this functionality in
		 * the distributed environment. We warn out here.
		 */
		if (IsA(parsetree, AlterTableMoveAllStmt))
		{
			ereport(WARNING, (errmsg("not propagating ALTER TABLE ALL IN TABLESPACE "
									 "commands to worker nodes"),
							  errhint("Connect to worker nodes directly to manually "
									  "move all tables.")));
		}
	}
	else
	{
		/*
		 * citus.enable_ddl_propagation is disabled, which means that PostgreSQL
		 * should handle the DDL command on a distributed table directly, without
		 * Citus intervening. The only exception is partition column drop, in
		 * which case we error out. Advanced Citus users use this to implement their
		 * own DDL propagation. We also use it to avoid re-propagating DDL commands
		 * when changing MX tables on workers. Below, we also make sure that DDL
		 * commands don't run queries that might get intercepted by Citus and error
		 * out, specifically we skip validation in foreign keys.
		 */

		if (IsA(parsetree, AlterTableStmt))
		{
			AlterTableStmt *alterTableStmt = (AlterTableStmt *) parsetree;
			if (alterTableStmt->relkind == OBJECT_TABLE)
			{
				ErrorIfAlterDropsPartitionColumn(alterTableStmt);

				/*
				 * When issuing an ALTER TABLE ... ADD FOREIGN KEY command, the
				 * the validation step should be skipped on the distributed table.
				 * Therefore, we check whether the given ALTER TABLE statement is a
				 * FOREIGN KEY constraint and if so disable the validation step.
				 * Note that validation is done on the shard level when DDL
				 * propagation is enabled. Unlike the preceeding Plan* calls, the
				 * following eagerly executes some tasks on workers.
				 */
				parsetree = WorkerProcessAlterTableStmt(alterTableStmt, queryString);
			}
		}
	}

	/* inform the user about potential caveats */
	if (IsA(parsetree, CreatedbStmt))
	{
		ereport(NOTICE, (errmsg("Citus partially supports CREATE DATABASE for "
								"distributed databases"),
						 errdetail("Citus does not propagate CREATE DATABASE "
								   "command to workers"),
						 errhint("You can manually create a database and its "
								 "extensions on workers.")));
	}
	else if (IsA(parsetree, CreateRoleStmt))
	{
		ereport(NOTICE, (errmsg("not propagating CREATE ROLE/USER commands to worker"
								" nodes"),
						 errhint("Connect to worker nodes directly to manually create all"
								 " necessary users and roles.")));
	}

	/*
	 * Make sure that on DROP DATABASE we terminate the background daemon
	 * associated with it.
	 */
	if (IsA(parsetree, DropdbStmt))
	{
		const bool missingOK = true;
		DropdbStmt *dropDbStatement = (DropdbStmt *) parsetree;
		char *dbname = dropDbStatement->dbname;
		Oid databaseOid = get_database_oid(dbname, missingOK);

		if (OidIsValid(databaseOid))
		{
			StopMaintenanceDaemon(databaseOid);
		}
	}

	/* set user if needed and go ahead and run local utility using standard hook */
	if (commandMustRunAsOwner)
	{
		GetUserIdAndSecContext(&savedUserId, &savedSecurityContext);
		SetUserIdAndSecContext(CitusExtensionOwner(), SECURITY_LOCAL_USERID_CHANGE);
	}

#if (PG_VERSION_NUM >= 100000)
	pstmt->utilityStmt = parsetree;
	standard_ProcessUtility(pstmt, queryString, context,
							params, queryEnv, dest, completionTag);
#else
	standard_ProcessUtility(parsetree, queryString, context,
							params, dest, completionTag);
#endif


	/*
	 * We only process CREATE TABLE ... PARTITION OF commands in the function below
	 * to handle the case when user creates a table as a partition of distributed table.
	 */
	if (IsA(parsetree, CreateStmt))
	{
		CreateStmt *createStatement = (CreateStmt *) parsetree;

		ProcessCreateTableStmtPartitionOf(createStatement);
	}

	/*
	 * We only process ALTER TABLE ... ATTACH PARTITION commands in the function below
	 * and distribute the partition if necessary.
	 */
	if (IsA(parsetree, AlterTableStmt))
	{
		AlterTableStmt *alterTableStatement = (AlterTableStmt *) parsetree;

		ProcessAlterTableStmtAttachPartition(alterTableStatement);
	}

	/* don't run post-process code for local commands */
	if (ddlJobs != NIL)
	{
		PostProcessUtility(parsetree);
	}

	if (commandMustRunAsOwner)
	{
		SetUserIdAndSecContext(savedUserId, savedSecurityContext);
	}

	/*
	 * Re-forming the foreign key graph relies on the command being executed
	 * on the local table first. However, in order to decide whether the
	 * command leads to an invalidation, we need to check before the command
	 * is being executed since we read pg_constraint table. Thus, we maintain a
	 * local flag and do the invalidation after multi_ProcessUtility,
	 * before ExecuteDistributedDDLJob().
	 */
	InvalidateForeignKeyGraphForDDL();

	/* after local command has completed, finish by executing worker DDLJobs, if any */
	if (ddlJobs != NIL)
	{
		ListCell *ddlJobCell = NULL;

		/*
		 * At this point, ALTER TABLE command has already run on the master, so we
		 * are checking constraints over the table with constraints already defined
		 * (to make the constraint check process same for ALTER TABLE and CREATE
		 * TABLE). If constraints do not fulfill the rules we defined, they will
		 * be removed and the table will return back to the state before the ALTER
		 * TABLE command.
		 */
		if (IsA(parsetree, AlterTableStmt))
		{
			AlterTableStmt *alterTableStatement = (AlterTableStmt *) parsetree;
			List *commandList = alterTableStatement->cmds;
			ListCell *commandCell = NULL;

			foreach(commandCell, commandList)
			{
				AlterTableCmd *command = (AlterTableCmd *) lfirst(commandCell);
				AlterTableType alterTableType = command->subtype;

				if (alterTableType == AT_AddConstraint)
				{
					LOCKMODE lockmode = NoLock;
					Oid relationId = InvalidOid;
					Constraint *constraint = NULL;

					Assert(list_length(commandList) == 1);

					ErrorIfUnsupportedAlterAddConstraintStmt(alterTableStatement);

					lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
					relationId = AlterTableLookupRelation(alterTableStatement, lockmode);

					if (!OidIsValid(relationId))
					{
						continue;
					}

					constraint = (Constraint *) command->def;
					if (constraint->contype == CONSTR_FOREIGN)
					{
						InvalidateForeignKeyGraph();
					}
				}
				else if (alterTableType == AT_AddColumn)
				{
					List *columnConstraints = NIL;
					ListCell *columnConstraint = NULL;
					Oid relationId = InvalidOid;
					LOCKMODE lockmode = NoLock;

					ColumnDef *columnDefinition = (ColumnDef *) command->def;
					columnConstraints = columnDefinition->constraints;
					if (columnConstraints)
					{
						ErrorIfUnsupportedAlterAddConstraintStmt(alterTableStatement);
					}

					lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
					relationId = AlterTableLookupRelation(alterTableStatement, lockmode);
					if (!OidIsValid(relationId))
					{
						continue;
					}

					foreach(columnConstraint, columnConstraints)
					{
						Constraint *constraint = (Constraint *) lfirst(columnConstraint);

						if (constraint->conname == NULL &&
							(constraint->contype == CONSTR_PRIMARY ||
							 constraint->contype == CONSTR_UNIQUE ||
							 constraint->contype == CONSTR_FOREIGN ||
							 constraint->contype == CONSTR_CHECK))
						{
							ErrorUnsupportedAlterTableAddColumn(relationId, command,
																constraint);
						}
					}
				}
			}
		}

		foreach(ddlJobCell, ddlJobs)
		{
			DDLJob *ddlJob = (DDLJob *) lfirst(ddlJobCell);

			ExecuteDistributedDDLJob(ddlJob);
		}
	}

	/* TODO: fold VACUUM's processing into the above block */
	if (IsA(parsetree, VacuumStmt))
	{
		VacuumStmt *vacuumStmt = (VacuumStmt *) parsetree;

		ProcessVacuumStmt(vacuumStmt, queryString);
	}

	/* warn for CLUSTER command on distributed tables */
	if (IsA(parsetree, ClusterStmt))
	{
		ClusterStmt *clusterStmt = (ClusterStmt *) parsetree;
		bool showPropagationWarning = false;

		/* CLUSTER all */
		if (clusterStmt->relation == NULL)
		{
			showPropagationWarning = true;
		}
		else
		{
			Oid relationId = InvalidOid;
			bool missingOK = false;

			relationId = RangeVarGetRelid(clusterStmt->relation, AccessShareLock,
										  missingOK);

			if (OidIsValid(relationId))
			{
				showPropagationWarning = IsDistributedTable(relationId);
			}
		}

		if (showPropagationWarning)
		{
			ereport(WARNING, (errmsg("not propagating CLUSTER command to worker nodes")));
		}
	}

	/*
	 * Ensure value is valid, we can't do some checks during CREATE
	 * EXTENSION. This is important to register some invalidation callbacks.
	 */
	CitusHasBeenLoaded();
}


static void
ErrorUnsupportedAlterTableAddColumn(Oid relationId, AlterTableCmd *command,
									Constraint *constraint)
{
	ColumnDef *columnDefinition = (ColumnDef *) command->def;
	char *colName = columnDefinition->colname;
	char *errMsg =
		"cannot execute ADD COLUMN command with PRIMARY KEY, UNIQUE, FOREIGN and CHECK constraints";
	StringInfo errHint = makeStringInfo();
	appendStringInfo(errHint, "You can issue each command separately such as ");
	appendStringInfo(errHint,
					 "ALTER TABLE %s ADD COLUMN %s data_type; ALTER TABLE %s ADD CONSTRAINT constraint_name ",
					 get_rel_name(relationId),
					 colName, get_rel_name(relationId));

	if (constraint->contype == CONSTR_UNIQUE)
	{
		appendStringInfo(errHint, "UNIQUE (%s)", colName);
	}
	else if (constraint->contype == CONSTR_PRIMARY)
	{
		appendStringInfo(errHint, "PRIMARY KEY (%s)", colName);
	}
	else if (constraint->contype == CONSTR_CHECK)
	{
		appendStringInfo(errHint, "CHECK (check_expression)");
	}
	else if (constraint->contype == CONSTR_FOREIGN)
	{
		RangeVar *referencedTable = constraint->pktable;
		char *referencedColumn = strVal(lfirst(list_head(constraint->pk_attrs)));
		Oid referencedRelationId = RangeVarGetRelid(referencedTable, NoLock, false);

		appendStringInfo(errHint, "FOREIGN KEY (%s) REFERENCES %s(%s)", colName,
						 get_rel_name(referencedRelationId), referencedColumn);

		if (constraint->fk_del_action == FKCONSTR_ACTION_SETNULL)
		{
			appendStringInfo(errHint, " %s", "ON DELETE SET NULL");
		}
		else if (constraint->fk_del_action == FKCONSTR_ACTION_CASCADE)
		{
			appendStringInfo(errHint, " %s", "ON DELETE CASCADE");
		}
		else if (constraint->fk_del_action == FKCONSTR_ACTION_SETDEFAULT)
		{
			appendStringInfo(errHint, " %s", "ON DELETE SET DEFAULT");
		}
		else if (constraint->fk_del_action == FKCONSTR_ACTION_RESTRICT)
		{
			appendStringInfo(errHint, " %s", "ON DELETE RESTRICT");
		}

		if (constraint->fk_upd_action == FKCONSTR_ACTION_SETNULL)
		{
			appendStringInfo(errHint, " %s", "ON UPDATE SET NULL");
		}
		else if (constraint->fk_upd_action == FKCONSTR_ACTION_CASCADE)
		{
			appendStringInfo(errHint, " %s", "ON UPDATE CASCADE");
		}
		else if (constraint->fk_upd_action == FKCONSTR_ACTION_SETDEFAULT)
		{
			appendStringInfo(errHint, " %s", "ON UPDATE SET DEFAULT");
		}
		else if (constraint->fk_upd_action == FKCONSTR_ACTION_RESTRICT)
		{
			appendStringInfo(errHint, " %s", "ON UPDATE RESTRICT");
		}
	}

	appendStringInfo(errHint, "%s", ";");

	ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					errmsg("%s", errMsg),
					errhint("%s", errHint->data),
					errdetail("Adding a column with a constraint in "
							  "one command is not supported because "
							  "all constraints in Citus must have "
							  "explicit names")));
}


/*
 * IsCitusExtensionStmt returns whether a given utility is a CREATE or ALTER
 * EXTENSION statement which references the citus extension. This function
 * returns false for all other inputs.
 */
static bool
IsCitusExtensionStmt(Node *parsetree)
{
	char *extensionName = "";

	if (IsA(parsetree, CreateExtensionStmt))
	{
		extensionName = ((CreateExtensionStmt *) parsetree)->extname;
	}
	else if (IsA(parsetree, AlterExtensionStmt))
	{
		extensionName = ((AlterExtensionStmt *) parsetree)->extname;
	}

	return (strcmp(extensionName, "citus") == 0);
}


/*
 * ProcessCreateTableStmtPartitionOf takes CreateStmt object as a parameter but
 * it only processes CREATE TABLE ... PARTITION OF statements and it checks if
 * user creates the table as a partition of a distributed table. In that case,
 * it distributes partition as well. Since the table itself is a partition,
 * CreateDistributedTable will attach it to its parent table automatically after
 * distributing it.
 *
 * This function does nothing if PostgreSQL's version is less then 10 and given
 * CreateStmt is not a CREATE TABLE ... PARTITION OF command.
 */
static void
ProcessCreateTableStmtPartitionOf(CreateStmt *createStatement)
{
#if (PG_VERSION_NUM >= 100000)
	if (createStatement->inhRelations != NIL && createStatement->partbound != NULL)
	{
		RangeVar *parentRelation = linitial(createStatement->inhRelations);
		bool parentMissingOk = false;
		Oid parentRelationId = RangeVarGetRelid(parentRelation, NoLock,
												parentMissingOk);

		/* a partition can only inherit from single parent table */
		Assert(list_length(createStatement->inhRelations) == 1);

		Assert(parentRelationId != InvalidOid);

		/*
		 * If a partition is being created and if its parent is a distributed
		 * table, we will distribute this table as well.
		 */
		if (IsDistributedTable(parentRelationId))
		{
			bool missingOk = false;
			Oid relationId = RangeVarGetRelid(createStatement->relation, NoLock,
											  missingOk);
			Var *parentDistributionColumn = DistPartitionKey(parentRelationId);
			char parentDistributionMethod = DISTRIBUTE_BY_HASH;
			char *parentRelationName = generate_qualified_relation_name(parentRelationId);
			bool viaDeprecatedAPI = false;

			CreateDistributedTable(relationId, parentDistributionColumn,
								   parentDistributionMethod, parentRelationName,
								   viaDeprecatedAPI);
		}
	}
#endif
}


/*
 * ProcessAlterTableStmtAttachPartition takes AlterTableStmt object as parameter
 * but it only processes into ALTER TABLE ... ATTACH PARTITION commands and
 * distributes the partition if necessary. There are four cases to consider;
 *
 * Parent is not distributed, partition is not distributed: We do not need to
 * do anything in this case.
 *
 * Parent is not distributed, partition is distributed: This can happen if
 * user first distributes a table and tries to attach it to a non-distributed
 * table. Non-distributed tables cannot have distributed partitions, thus we
 * simply error out in this case.
 *
 * Parent is distributed, partition is not distributed: We should distribute
 * the table and attach it to its parent in workers. CreateDistributedTable
 * perform both of these operations. Thus, we will not propagate ALTER TABLE
 * ... ATTACH PARTITION command to workers.
 *
 * Parent is distributed, partition is distributed: Partition is already
 * distributed, we only need to attach it to its parent in workers. Attaching
 * operation will be performed via propagating this ALTER TABLE ... ATTACH
 * PARTITION command to workers.
 *
 * This function does nothing if PostgreSQL's version is less then 10 and given
 * CreateStmt is not a ALTER TABLE ... ATTACH PARTITION OF command.
 */
static void
ProcessAlterTableStmtAttachPartition(AlterTableStmt *alterTableStatement)
{
#if (PG_VERSION_NUM >= 100000)
	List *commandList = alterTableStatement->cmds;
	ListCell *commandCell = NULL;

	foreach(commandCell, commandList)
	{
		AlterTableCmd *alterTableCommand = (AlterTableCmd *) lfirst(commandCell);

		if (alterTableCommand->subtype == AT_AttachPartition)
		{
			Oid relationId = AlterTableLookupRelation(alterTableStatement, NoLock);
			PartitionCmd *partitionCommand = (PartitionCmd *) alterTableCommand->def;
			bool partitionMissingOk = false;
			Oid partitionRelationId = RangeVarGetRelid(partitionCommand->name, NoLock,
													   partitionMissingOk);

			/*
			 * If user first distributes the table then tries to attach it to non
			 * distributed table, we error out.
			 */
			if (!IsDistributedTable(relationId) &&
				IsDistributedTable(partitionRelationId))
			{
				char *parentRelationName = get_rel_name(partitionRelationId);

				ereport(ERROR, (errmsg("non-distributed tables cannot have "
									   "distributed partitions"),
								errhint("Distribute the partitioned table \"%s\" "
										"instead", parentRelationName)));
			}

			/* if parent of this table is distributed, distribute this table too */
			if (IsDistributedTable(relationId) &&
				!IsDistributedTable(partitionRelationId))
			{
				Var *distributionColumn = DistPartitionKey(relationId);
				char distributionMethod = DISTRIBUTE_BY_HASH;
				char *parentRelationName = generate_qualified_relation_name(relationId);
				bool viaDeprecatedAPI = false;

				CreateDistributedTable(partitionRelationId, distributionColumn,
									   distributionMethod, parentRelationName,
									   viaDeprecatedAPI);
			}
		}
	}
#endif
}


/*
 * PlanRenameStmt first determines whether a given rename statement involves
 * a distributed table. If so (and if it is supported, i.e. renames a column),
 * it creates a DDLJob to encapsulate information needed during the worker node
 * portion of DDL execution before returning that DDLJob in a List. If no dis-
 * tributed table is involved, this function returns NIL.
 */
static List *
PlanRenameStmt(RenameStmt *renameStmt, const char *renameCommand)
{
	Oid objectRelationId = InvalidOid; /* SQL Object OID */
	Oid tableRelationId = InvalidOid; /* Relation OID, maybe not the same. */
	bool isDistributedRelation = false;
	DDLJob *ddlJob = NULL;

	/*
	 * We only support some of the PostgreSQL supported RENAME statements, and
	 * our list include only renaming table and index (related) objects.
	 */
	if (!IsAlterTableRenameStmt(renameStmt) &&
		!IsIndexRenameStmt(renameStmt) &&
		!IsPolicyRenameStmt(renameStmt))
	{
		return NIL;
	}

	/*
	 * The lock levels here should be same as the ones taken in
	 * RenameRelation(), renameatt() and RenameConstraint(). However, since all
	 * three statements have identical lock levels, we just use a single statement.
	 */
	objectRelationId = RangeVarGetRelid(renameStmt->relation,
										AccessExclusiveLock,
										renameStmt->missing_ok);

	/*
	 * If the table does not exist, don't do anything here to allow PostgreSQL
	 * to throw the appropriate error or notice message later.
	 */
	if (!OidIsValid(objectRelationId))
	{
		return NIL;
	}

	/* we have no planning to do unless the table is distributed */
	switch (renameStmt->renameType)
	{
		case OBJECT_TABLE:
		case OBJECT_COLUMN:
		case OBJECT_TABCONSTRAINT:
		case OBJECT_POLICY:
		{
			/* the target object is our tableRelationId. */
			tableRelationId = objectRelationId;
			break;
		}

		case OBJECT_INDEX:
		{
			/*
			 * here, objRelationId points to the index relation entry, and we
			 * are interested into the entry of the table on which the index is
			 * defined.
			 */
			tableRelationId = IndexGetRelation(objectRelationId, false);
			break;
		}

		default:

			/*
			 * Nodes that are not supported by Citus: we pass-through to the
			 * main PostgreSQL executor. Any Citus-supported RenameStmt
			 * renameType must appear above in the switch, explicitly.
			 */
			return NIL;
	}

	isDistributedRelation = IsDistributedTable(tableRelationId);
	if (!isDistributedRelation)
	{
		return NIL;
	}

	/*
	 * We might ERROR out on some commands, but only for Citus tables where
	 * isDistributedRelation is true. That's why this test comes this late in
	 * the function.
	 */
	ErrorIfUnsupportedRenameStmt(renameStmt);

	ddlJob = palloc0(sizeof(DDLJob));
	ddlJob->targetRelationId = tableRelationId;
	ddlJob->concurrentIndexCmd = false;
	ddlJob->commandString = renameCommand;
	ddlJob->taskList = DDLTaskList(tableRelationId, renameCommand);

	return list_make1(ddlJob);
}


/*
 * PlanAlterObjectSchemaStmt determines whether a given ALTER ... SET SCHEMA
 * statement involves a distributed table and issues a warning if so. Because
 * we do not support distributed ALTER ... SET SCHEMA, this function always
 * returns NIL.
 */
static List *
PlanAlterObjectSchemaStmt(AlterObjectSchemaStmt *alterObjectSchemaStmt,
						  const char *alterObjectSchemaCommand)
{
	Oid relationId = InvalidOid;

	if (alterObjectSchemaStmt->relation == NULL)
	{
		return NIL;
	}

	relationId = RangeVarGetRelid(alterObjectSchemaStmt->relation,
								  AccessExclusiveLock,
								  alterObjectSchemaStmt->missing_ok);

	/* first check whether a distributed relation is affected */
	if (!OidIsValid(relationId) || !IsDistributedTable(relationId))
	{
		return NIL;
	}

	/* emit a warning if a distributed relation is affected */
	ereport(WARNING, (errmsg("not propagating ALTER ... SET SCHEMA commands to "
							 "worker nodes"),
					  errhint("Connect to worker nodes directly to manually "
							  "change schemas of affected objects.")));

	return NIL;
}


/*
 * ErrorIfUnstableCreateOrAlterExtensionStmt compares CITUS_EXTENSIONVERSION
 * and version given CREATE/ALTER EXTENSION statement will create/update to. If
 * they are not same in major or minor version numbers, this function errors
 * out. It ignores the schema version.
 */
static void
ErrorIfUnstableCreateOrAlterExtensionStmt(Node *parsetree)
{
	char *newExtensionVersion = ExtractNewExtensionVersion(parsetree);

	if (newExtensionVersion != NULL)
	{
		/*  explicit version provided in CREATE or ALTER EXTENSION UPDATE; verify */
		if (!MajorVersionsCompatible(newExtensionVersion, CITUS_EXTENSIONVERSION))
		{
			ereport(ERROR, (errmsg("specified version incompatible with loaded "
								   "Citus library"),
							errdetail("Loaded library requires %s, but %s was specified.",
									  CITUS_MAJORVERSION, newExtensionVersion),
							errhint("If a newer library is present, restart the database "
									"and try the command again.")));
		}
	}
	else
	{
		/*
		 * No version was specified, so PostgreSQL will use the default_version
		 * from the citus.control file.
		 */
		CheckAvailableVersion(ERROR);
	}
}


/*
 * ExtractNewExtensionVersion returns the new extension version specified by
 * a CREATE or ALTER EXTENSION statement. Other inputs are not permitted. This
 * function returns NULL for statements with no explicit version specified.
 */
static char *
ExtractNewExtensionVersion(Node *parsetree)
{
	char *newVersion = NULL;
	List *optionsList = NIL;
	ListCell *optionsCell = NULL;

	if (IsA(parsetree, CreateExtensionStmt))
	{
		optionsList = ((CreateExtensionStmt *) parsetree)->options;
	}
	else if (IsA(parsetree, AlterExtensionStmt))
	{
		optionsList = ((AlterExtensionStmt *) parsetree)->options;
	}
	else
	{
		/* input must be one of the two above types */
		Assert(false);
	}

	foreach(optionsCell, optionsList)
	{
		DefElem *defElement = (DefElem *) lfirst(optionsCell);
		if (strncmp(defElement->defname, "new_version", NAMEDATALEN) == 0)
		{
			newVersion = strVal(defElement->arg);
			break;
		}
	}

	return newVersion;
}


/*
 * ErrorIfUnsopprtedAlterAddConstraintStmt runs the constraint checks on distributed
 * table using the same logic with create_distributed_table.
 */
static void
ErrorIfUnsupportedAlterAddConstraintStmt(AlterTableStmt *alterTableStatement)
{
	LOCKMODE lockmode = AlterTableGetLockLevel(alterTableStatement->cmds);
	Oid relationId = AlterTableLookupRelation(alterTableStatement, lockmode);
	char distributionMethod = PartitionMethod(relationId);
	Var *distributionColumn = DistPartitionKey(relationId);
	uint32 colocationId = TableColocationId(relationId);
	Relation relation = relation_open(relationId, ExclusiveLock);

	ErrorIfUnsupportedConstraint(relation, distributionMethod, distributionColumn,
								 colocationId);
	relation_close(relation, NoLock);
}


/*
 * ErrorIfUnsupportedConstraint run checks related to unique index / exclude
 * constraints.
 *
 * The function skips the uniqeness checks for reference tables (i.e., distribution
 * method is 'none').
 *
 * Forbid UNIQUE, PRIMARY KEY, or EXCLUDE constraints on append partitioned
 * tables, since currently there is no way of enforcing uniqueness for
 * overlapping shards.
 *
 * Similarly, do not allow such constraints if they do not include partition
 * column. This check is important for two reasons:
 * i. First, currently Citus does not enforce uniqueness constraint on multiple
 * shards.
 * ii. Second, INSERT INTO .. ON CONFLICT (i.e., UPSERT) queries can be executed
 * with no further check for constraints.
 */
void
ErrorIfUnsupportedConstraint(Relation relation, char distributionMethod,
							 Var *distributionColumn, uint32 colocationId)
{
	char *relationName = NULL;
	List *indexOidList = NULL;
	ListCell *indexOidCell = NULL;

	/*
	 * We first perform check for foreign constraints. It is important to do this check
	 * before next check, because other types of constraints are allowed on reference
	 * tables and we return early for those constraints thanks to next check. Therefore,
	 * for reference tables, we first check for foreing constraints and if they are OK,
	 * we do not error out for other types of constraints.
	 */
	ErrorIfUnsupportedForeignConstraint(relation, distributionMethod, distributionColumn,
										colocationId);

	/*
	 * Citus supports any kind of uniqueness constraints for reference tables
	 * given that they only consist of a single shard and we can simply rely on
	 * Postgres.
	 */
	if (distributionMethod == DISTRIBUTE_BY_NONE)
	{
		return;
	}

	relationName = RelationGetRelationName(relation);
	indexOidList = RelationGetIndexList(relation);

	foreach(indexOidCell, indexOidList)
	{
		Oid indexOid = lfirst_oid(indexOidCell);
		Relation indexDesc = index_open(indexOid, RowExclusiveLock);
		IndexInfo *indexInfo = NULL;
		AttrNumber *attributeNumberArray = NULL;
		bool hasDistributionColumn = false;
		int attributeCount = 0;
		int attributeIndex = 0;

		/* extract index key information from the index's pg_index info */
		indexInfo = BuildIndexInfo(indexDesc);

		/* only check unique indexes and exclusion constraints. */
		if (indexInfo->ii_Unique == false && indexInfo->ii_ExclusionOps == NULL)
		{
			index_close(indexDesc, NoLock);
			continue;
		}

		/*
		 * Citus cannot enforce uniqueness/exclusion constraints with overlapping shards.
		 * Thus, emit a warning for unique indexes and exclusion constraints on
		 * append partitioned tables.
		 */
		if (distributionMethod == DISTRIBUTE_BY_APPEND)
		{
			ereport(WARNING, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							  errmsg("table \"%s\" has a UNIQUE or EXCLUDE constraint",
									 relationName),
							  errdetail("UNIQUE constraints, EXCLUDE constraints, "
										"and PRIMARY KEYs on "
										"append-partitioned tables cannot be enforced."),
							  errhint("Consider using hash partitioning.")));
		}

		attributeCount = indexInfo->ii_NumIndexAttrs;
		attributeNumberArray = IndexInfoAttributeNumberArray(indexInfo);

		for (attributeIndex = 0; attributeIndex < attributeCount; attributeIndex++)
		{
			AttrNumber attributeNumber = attributeNumberArray[attributeIndex];
			bool uniqueConstraint = false;
			bool exclusionConstraintWithEquality = false;

			if (distributionColumn->varattno != attributeNumber)
			{
				continue;
			}

			uniqueConstraint = indexInfo->ii_Unique;
			exclusionConstraintWithEquality = (indexInfo->ii_ExclusionOps != NULL &&
											   OperatorImplementsEquality(
												   indexInfo->ii_ExclusionOps[
													   attributeIndex]));

			if (uniqueConstraint || exclusionConstraintWithEquality)
			{
				hasDistributionColumn = true;
				break;
			}
		}

		if (!hasDistributionColumn)
		{
			ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
							errmsg("cannot create constraint on \"%s\"",
								   relationName),
							errdetail("Distributed relations cannot have UNIQUE, "
									  "EXCLUDE, or PRIMARY KEY constraints that do not "
									  "include the partition column (with an equality "
									  "operator if EXCLUDE).")));
		}

		index_close(indexDesc, NoLock);
	}
}


/*
 * ErrorIfDistributedRenameStmt errors out if the corresponding rename statement
 * operates on any part of a distributed table other than a column.
 *
 * Note: This function handles RenameStmt applied to relations handed by Citus.
 * At the moment of writing this comment, it could be either tables or indexes.
 */
static void
ErrorIfUnsupportedRenameStmt(RenameStmt *renameStmt)
{
	if (IsAlterTableRenameStmt(renameStmt) &&
		renameStmt->renameType == OBJECT_TABCONSTRAINT)
	{
		ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						errmsg("renaming constraints belonging to distributed tables is "
							   "currently unsupported")));
	}
}


/*
 * IsAlterTableRenameStmt returns whether the passed-in RenameStmt is one of
 * the following forms:
 *
 *   - ALTER TABLE RENAME
 *   - ALTER TABLE RENAME COLUMN
 *   - ALTER TABLE RENAME CONSTRAINT
 */
static bool
IsAlterTableRenameStmt(RenameStmt *renameStmt)
{
	bool isAlterTableRenameStmt = false;

	if (renameStmt->renameType == OBJECT_TABLE)
	{
		isAlterTableRenameStmt = true;
	}
	else if (renameStmt->renameType == OBJECT_COLUMN &&
			 renameStmt->relationType == OBJECT_TABLE)
	{
		isAlterTableRenameStmt = true;
	}
	else if (renameStmt->renameType == OBJECT_TABCONSTRAINT)
	{
		isAlterTableRenameStmt = true;
	}

	return isAlterTableRenameStmt;
}


/*
 * IsIndexRenameStmt returns whether the passed-in RenameStmt is the following
 * form:
 *
 *   - ALTER INDEX RENAME
 */
static bool
IsIndexRenameStmt(RenameStmt *renameStmt)
{
	bool isIndexRenameStmt = false;

	if (renameStmt->renameType == OBJECT_INDEX)
	{
		isIndexRenameStmt = true;
	}

	return isIndexRenameStmt;
}


/*
 * ExecuteDistributedDDLJob simply executes a provided DDLJob in a distributed trans-
 * action, including metadata sync if needed. If the multi shard commit protocol is
 * in its default value of '1pc', then a notice message indicating that '2pc' might be
 * used for extra safety. In the commit protocol, a BEGIN is sent after connection to
 * each shard placement and COMMIT/ROLLBACK is handled by
 * CoordinatedTransactionCallback function.
 *
 * The function errors out if the node is not the coordinator or if the DDL is on
 * a partitioned table which has replication factor > 1.
 *
 */
static void
ExecuteDistributedDDLJob(DDLJob *ddlJob)
{
	bool shouldSyncMetadata = ShouldSyncTableMetadata(ddlJob->targetRelationId);

	EnsureCoordinator();
	EnsurePartitionTableNotReplicated(ddlJob->targetRelationId);

	if (!ddlJob->concurrentIndexCmd)
	{
		if (shouldSyncMetadata)
		{
			char *setSearchPathCommand = SetSearchPathToCurrentSearchPathCommand();

			SendCommandToWorkers(WORKERS_WITH_METADATA, DISABLE_DDL_PROPAGATION);

			/*
			 * Given that we're relaying the query to the worker nodes directly,
			 * we should set the search path exactly the same when necessary.
			 */
			if (setSearchPathCommand != NULL)
			{
				SendCommandToWorkers(WORKERS_WITH_METADATA, setSearchPathCommand);
			}

			SendCommandToWorkers(WORKERS_WITH_METADATA, (char *) ddlJob->commandString);
		}

		if (MultiShardConnectionType == SEQUENTIAL_CONNECTION ||
			ddlJob->executeSequentially)
		{
			ExecuteModifyTasksSequentiallyWithoutResults(ddlJob->taskList, CMD_UTILITY);
		}
		else
		{
			ExecuteModifyTasksWithoutResults(ddlJob->taskList);
		}
	}
	else
	{
		/* save old commit protocol to restore at xact end */
		Assert(SavedMultiShardCommitProtocol == COMMIT_PROTOCOL_BARE);
		SavedMultiShardCommitProtocol = MultiShardCommitProtocol;
		MultiShardCommitProtocol = COMMIT_PROTOCOL_BARE;

		PG_TRY();
		{
			ExecuteModifyTasksSequentiallyWithoutResults(ddlJob->taskList, CMD_UTILITY);

			if (shouldSyncMetadata)
			{
				List *commandList = list_make1(DISABLE_DDL_PROPAGATION);
				char *setSearchPathCommand = SetSearchPathToCurrentSearchPathCommand();

				/*
				 * Given that we're relaying the query to the worker nodes directly,
				 * we should set the search path exactly the same when necessary.
				 */
				if (setSearchPathCommand != NULL)
				{
					commandList = lappend(commandList, setSearchPathCommand);
				}

				commandList = lappend(commandList, (char *) ddlJob->commandString);

				SendBareCommandListToWorkers(WORKERS_WITH_METADATA, commandList);
			}
		}
		PG_CATCH();
		{
			ereport(ERROR,
					(errmsg("CONCURRENTLY-enabled index command failed"),
					 errdetail("CONCURRENTLY-enabled index commands can fail partially, "
							   "leaving behind an INVALID index."),
					 errhint("Use DROP INDEX CONCURRENTLY IF EXISTS to remove the "
							 "invalid index, then retry the original command.")));
		}
		PG_END_TRY();
	}
}


/*
 * SetSearchPathToCurrentSearchPathCommand generates a command which can
 * set the search path to the exact same search path that the issueing node
 * has.
 *
 * If the current search path is null (or doesn't have any valid schemas),
 * the function returns NULL.
 */
static char *
SetSearchPathToCurrentSearchPathCommand(void)
{
	StringInfo setCommand = NULL;
	char *currentSearchPath = CurrentSearchPath();

	if (currentSearchPath == NULL)
	{
		return NULL;
	}

	setCommand = makeStringInfo();
	appendStringInfo(setCommand, "SET search_path TO %s;", currentSearchPath);

	return setCommand->data;
}


/*
 * CurrentSearchPath is a C interface for calling current_schemas(bool) that
 * PostgreSQL exports.
 *
 * CurrentSchemas returns all the schemas in the seach_path that are seperated
 * with comma (,) sign. The returned string can be used to set the search_path.
 *
 * The function omits implicit schemas.
 *
 * The function returns NULL if there are no valid schemas in the search_path,
 * mimicing current_schemas(false) function.
 */
static char *
CurrentSearchPath(void)
{
	StringInfo currentSearchPath = makeStringInfo();
	List *searchPathList = fetch_search_path(false);
	ListCell *searchPathCell;
	bool schemaAdded = false;

	foreach(searchPathCell, searchPathList)
	{
		char *schemaName = get_namespace_name(lfirst_oid(searchPathCell));

		/* watch out for deleted namespace */
		if (schemaName)
		{
			if (schemaAdded)
			{
				appendStringInfoString(currentSearchPath, ",");
				schemaAdded = false;
			}

			appendStringInfoString(currentSearchPath, quote_identifier(schemaName));
			schemaAdded = true;
		}
	}

	/* fetch_search_path() returns a palloc'd list that we should free now */
	list_free(searchPathList);

	return (currentSearchPath->len > 0 ? currentSearchPath->data : NULL);
}


/*
 * PostProcessUtility performs additional tasks after a utility's local portion
 * has been completed. Right now, the sole use is marking new indexes invalid
 * if they were created using the CONCURRENTLY flag. This (non-transactional)
 * change provides the fallback state if an error is raised, otherwise a sub-
 * sequent change to valid will be committed.
 */
static void
PostProcessUtility(Node *parsetree)
{
	IndexStmt *indexStmt = NULL;
	Relation relation = NULL;
	Oid indexRelationId = InvalidOid;
	Relation indexRelation = NULL;
	Relation pg_index = NULL;
	HeapTuple indexTuple = NULL;
	Form_pg_index indexForm = NULL;

	/* only IndexStmts are processed */
	if (!IsA(parsetree, IndexStmt))
	{
		return;
	}

	/* and even then only if they're CONCURRENT */
	indexStmt = (IndexStmt *) parsetree;
	if (!indexStmt->concurrent)
	{
		return;
	}

	/* finally, this logic only applies to the coordinator */
	if (!IsCoordinator())
	{
		return;
	}

	/* commit the current transaction and start anew */
	CommitTransactionCommand();
	StartTransactionCommand();

	/* get the affected relation and index */
	relation = heap_openrv(indexStmt->relation, ShareUpdateExclusiveLock);
	indexRelationId = get_relname_relid(indexStmt->idxname,
										RelationGetNamespace(relation));
	indexRelation = index_open(indexRelationId, RowExclusiveLock);

	/* close relations but retain locks */
	heap_close(relation, NoLock);
	index_close(indexRelation, NoLock);

	/* mark index as invalid, in-place (cannot be rolled back) */
	index_set_state_flags(indexRelationId, INDEX_DROP_CLEAR_VALID);

	/* re-open a transaction command from here on out */
	CommitTransactionCommand();
	StartTransactionCommand();

	/* now, update index's validity in a way that can roll back */
	pg_index = heap_open(IndexRelationId, RowExclusiveLock);

	indexTuple = SearchSysCacheCopy1(INDEXRELID, ObjectIdGetDatum(indexRelationId));
	Assert(HeapTupleIsValid(indexTuple)); /* better be present, we have lock! */

	/* mark as valid, save, and update pg_index indexes */
	indexForm = (Form_pg_index) GETSTRUCT(indexTuple);
	indexForm->indisvalid = true;

	CatalogTupleUpdate(pg_index, &indexTuple->t_self, indexTuple);

	/* clean up; index now marked valid, but ROLLBACK will mark invalid */
	heap_freetuple(indexTuple);
	heap_close(pg_index, RowExclusiveLock);
}


/*
 * PlanGrantStmt determines whether a given GRANT/REVOKE statement involves
 * a distributed table. If so, it creates DDLJobs to encapsulate information
 * needed during the worker node portion of DDL execution before returning the
 * DDLJobs in a List. If no distributed table is involved, this returns NIL.
 *
 * NB: So far column level privileges are not supported.
 */
List *
PlanGrantStmt(GrantStmt *grantStmt)
{
	StringInfoData privsString;
	StringInfoData granteesString;
	StringInfoData targetString;
	StringInfoData ddlString;
	ListCell *granteeCell = NULL;
	List *tableIdList = NIL;
	ListCell *tableListCell = NULL;
	bool isFirst = true;
	List *ddlJobs = NIL;

	initStringInfo(&privsString);
	initStringInfo(&granteesString);
	initStringInfo(&targetString);
	initStringInfo(&ddlString);

	/*
	 * So far only table level grants are supported. Most other types of
	 * grants aren't interesting anyway.
	 */
	if (grantStmt->objtype != RELATION_OBJECT_TYPE)
	{
		return NIL;
	}

	tableIdList = CollectGrantTableIdList(grantStmt);

	/* nothing to do if there is no distributed table in the grant list */
	if (tableIdList == NIL)
	{
		return NIL;
	}

	/* deparse the privileges */
	if (grantStmt->privileges == NIL)
	{
		appendStringInfo(&privsString, "ALL");
	}
	else
	{
		ListCell *privilegeCell = NULL;

		isFirst = true;
		foreach(privilegeCell, grantStmt->privileges)
		{
			AccessPriv *priv = lfirst(privilegeCell);

			if (!isFirst)
			{
				appendStringInfoString(&privsString, ", ");
			}
			isFirst = false;

			if (priv->cols != NIL)
			{
				ereport(ERROR, (errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
								errmsg("grant/revoke on column list is currently "
									   "unsupported")));
			}

			Assert(priv->priv_name != NULL);

			appendStringInfo(&privsString, "%s", priv->priv_name);
		}
	}

	/* deparse the grantees */
	isFirst = true;
	foreach(granteeCell, grantStmt->grantees)
	{
		RoleSpec *spec = lfirst(granteeCell);

		if (!isFirst)
		{
			appendStringInfoString(&granteesString, ", ");
		}
		isFirst = false;

		appendStringInfoString(&granteesString, RoleSpecString(spec));
	}

	/*
	 * Deparse the target objects, and issue the deparsed statements to
	 * workers, if applicable. That's so we easily can replicate statements
	 * only to distributed relations.
	 */
	isFirst = true;
	foreach(tableListCell, tableIdList)
	{
		Oid relationId = lfirst_oid(tableListCell);
		const char *grantOption = "";
		DDLJob *ddlJob = NULL;

		Assert(IsDistributedTable(relationId));

		resetStringInfo(&targetString);
		appendStringInfo(&targetString, "%s", generate_relation_name(relationId, NIL));

		if (grantStmt->is_grant)
		{
			if (grantStmt->grant_option)
			{
				grantOption = " WITH GRANT OPTION";
			}

			appendStringInfo(&ddlString, "GRANT %s ON %s TO %s%s",
							 privsString.data, targetString.data, granteesString.data,
							 grantOption);
		}
		else
		{
			if (grantStmt->grant_option)
			{
				grantOption = "GRANT OPTION FOR ";
			}

			appendStringInfo(&ddlString, "REVOKE %s%s ON %s FROM %s",
							 grantOption, privsString.data, targetString.data,
							 granteesString.data);
		}

		ddlJob = palloc0(sizeof(DDLJob));
		ddlJob->targetRelationId = relationId;
		ddlJob->concurrentIndexCmd = false;
		ddlJob->commandString = pstrdup(ddlString.data);
		ddlJob->taskList = DDLTaskList(relationId, ddlString.data);

		ddlJobs = lappend(ddlJobs, ddlJob);

		resetStringInfo(&ddlString);
	}

	return ddlJobs;
}


/*
 *  CollectGrantTableIdList determines and returns a list of distributed table
 *  Oids from grant statement.
 *  Grant statement may appear in two forms
 *  1 - grant on table:
 *      each distributed table oid in grant object list is added to returned list.
 *  2 - grant all tables in schema:
 *     Collect namespace oid list from grant statement
 *     Add each distributed table oid in the target namespace list to the returned list.
 */
static List *
CollectGrantTableIdList(GrantStmt *grantStmt)
{
	List *grantTableList = NIL;
	bool grantOnTableCommand = false;
	bool grantAllTablesOnSchemaCommand = false;

	grantOnTableCommand = (grantStmt->targtype == ACL_TARGET_OBJECT &&
						   grantStmt->objtype == RELATION_OBJECT_TYPE);
	grantAllTablesOnSchemaCommand = (grantStmt->targtype == ACL_TARGET_ALL_IN_SCHEMA &&
									 grantStmt->objtype == RELATION_OBJECT_TYPE);

	/* we are only interested in table level grants */
	if (!grantOnTableCommand && !grantAllTablesOnSchemaCommand)
	{
		return NIL;
	}

	if (grantAllTablesOnSchemaCommand)
	{
		List *distTableOidList = DistTableOidList();
		ListCell *distributedTableOidCell = NULL;
		List *namespaceOidList = NIL;

		ListCell *objectCell = NULL;
		foreach(objectCell, grantStmt->objects)
		{
			char *nspname = strVal(lfirst(objectCell));
			bool missing_ok = false;
			Oid namespaceOid = get_namespace_oid(nspname, missing_ok);
			Assert(namespaceOid != InvalidOid);
			namespaceOidList = list_append_unique_oid(namespaceOidList, namespaceOid);
		}

		foreach(distributedTableOidCell, distTableOidList)
		{
			Oid relationId = lfirst_oid(distributedTableOidCell);
			Oid namespaceOid = get_rel_namespace(relationId);
			if (list_member_oid(namespaceOidList, namespaceOid))
			{
				grantTableList = lappend_oid(grantTableList, relationId);
			}
		}
	}
	else
	{
		ListCell *objectCell = NULL;
		foreach(objectCell, grantStmt->objects)
		{
			RangeVar *relvar = (RangeVar *) lfirst(objectCell);
			Oid relationId = RangeVarGetRelid(relvar, NoLock, false);
			if (IsDistributedTable(relationId))
			{
				grantTableList = lappend_oid(grantTableList, relationId);
			}
		}
	}

	return grantTableList;
}


/*
 * RoleSpecString resolves the role specification to its string form that is suitable for transport to a worker node.
 * This function resolves the following identifiers from the current context so they are safe to transfer.
 *
 * CURRENT_USER - resolved to the user name of the current role being used
 * SESSION_USER - resolved to the user name of the user that opened the session
 */
const char *
RoleSpecString(RoleSpec *spec)
{
	switch (spec->roletype)
	{
		case ROLESPEC_CSTRING:
		{
			return quote_identifier(spec->rolename);
		}

		case ROLESPEC_CURRENT_USER:
		{
			return quote_identifier(GetUserNameFromId(GetUserId(), false));
		}

		case ROLESPEC_SESSION_USER:
		{
			return quote_identifier(GetUserNameFromId(GetSessionUserId(), false));
		}

		case ROLESPEC_PUBLIC:
		{
			return "PUBLIC";
		}

		default:
		{
			elog(ERROR, "unexpected role type %d", spec->roletype);
		}
	}
}
