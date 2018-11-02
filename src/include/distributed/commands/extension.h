/*-------------------------------------------------------------------------
 *
 * extension.c
 *    Declarations for public functions and variables use for altering and
 *    creating extensions.
 *
 * Copyright (c) 2018, Citus Data, Inc.
 *
 *-------------------------------------------------------------------------
 */

#ifndef CITUS_EXTENSION_H
#define CITUS_EXTENSION_H

#include "c.h"

#include "nodes/parsenodes.h"

extern bool IsCitusExtensionStmt(Node *parsetree);
extern void ErrorIfUnstableCreateOrAlterExtensionStmt(Node *parsetree);

#endif /*CITUS_EXTENSION_H */
