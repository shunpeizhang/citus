/*-------------------------------------------------------------------------
 *
 * vacuum.h
 *    Declarations for public functions and variables use for vacuuming
 *    distributed tables.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_VACUUM_H
#define CITUS_VACUUM_H

#include "nodes/parsenodes.h"

extern void ProcessVacuumStmt(VacuumStmt *vacuumStmt, const char *vacuumCommand);

#endif /*CITUS_VACUUM_H */
