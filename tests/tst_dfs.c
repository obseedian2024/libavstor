/*
* This file is part of libavstor.
*
* BSD 3-Clause License
*
* Copyright (c) 2025, Tamas Fejerpataky
*
* Redistribution and use in source and binary forms, with or without
* modification, are permitted provided that the following conditions are met:
*
* 1. Redistributions of source code must retain the above copyright notice, this
*    list of conditions and the following disclaimer.
*
* 2. Redistributions in binary form must reproduce the above copyright notice,
*    this list of conditions and the following disclaimer in the documentation
*    and/or other materials provided with the distribution.
*
* 3. Neither the name of the copyright holder nor the names of its
*    contributors may be used to endorse or promote products derived from
*    this software without specific prior written permission.
*
* THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
* AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
* IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
* DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
* FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
* DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
* SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
* CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
* OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
* OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include <avstor.h>

#include "avsdb.h"
#include "avstest.h"

struct dfs_create_db_param {
    const char        *filename;
    unsigned    cache_size;
    int         level_count;
    long        *child_count;
};

static int dfs_create_db(void *param)
{
    avstor_key key;
    struct dfs_create_db_param *p = (struct dfs_create_db_param*)param;

    /* depth-first stack and pointer to top */
    struct st_elem {
        avstor_node node;
        int32_t     next_key;
    } *st, *top;
    avstor *db;
    long total_nodes = 0;
    long nodes_expected = 0;
    long nodes_per_level = 1;
    int i, res, result, level = 0;

    /* Calculate expected number of nodes to be created */
    for (i = 0; i < p->level_count; i++) {       
        nodes_per_level *= p->child_count[i];
        nodes_expected += nodes_per_level;
    }

    /* allocate stack for a depth-first insertion algorithm */
    st = calloc(p->level_count, sizeof(struct st_elem));
    if (!st) {
        printf("%sERROR: calloc failed%s\n", YEL, CRESET);
        return 0;
    }

    /* create (or overwrite) a new database file */
    if (AVSTOR_OK != (res = avstor_open(&db, p->filename, p->cache_size,
                                        AVSTOR_OPEN_CREATE | AVSTOR_OPEN_READWRITE | AVSTOR_OPEN_AUTOSAVE))) {
        printf("%sERROR: avstor_open failed with %i%s\n", YEL, res, CRESET);
        free(st);
        return 0;
    }

    /* initialize top of stack (i.e. the top level in the hierarchy) */
    top = &st[0];
    avstor_node_init(db, &top->node);

    /* key length and comparer will be the same for all subtrees */
    key.len = sizeof(AvsDbIntRec);
    key.comparer = &AvsIntNode_comparer;

    while (1) {
        avstor_node new_node, *p_new_node;
        AvsDbIntRec dbnode;
        if (top->next_key == p->child_count[level]) {
            /* finished creating subtree, move back up to parent tree */
            avstor_node_destroy(&top->node);

            /* if top level is done, quit*/
            if (--level < 0) break;

            top = &st[level];
        }
        else {
            /* create the node with sequential int keys within the tree */
            dbnode.key = top->next_key;
            dbnode.data = total_nodes;
            key.buf = &dbnode;

            /* only need to return new node if we must create children for it (i.e. non-leaf) */
            p_new_node = (level < (p->level_count - 1)) ? &new_node : NULL;

            if (AVSTOR_OK != (res = avstor_create_key(&top->node, &key, p_new_node))) {
                printf("%sERROR: avstor_create_key failed with %i%s\n", YEL, res, CRESET);
                if (p_new_node && (res == AVSTOR_EXISTS)) {
                    /* new_node is valid if we get this error, so destroy */
                    avstor_node_destroy(p_new_node);
                }
                result = 0;
                goto close_and_return;
            }
            total_nodes++;
            top->next_key++;

            /* Create children first, before creating siblings */
            if (p_new_node) {
                st[level++].node = top->node;
                top = &st[level];
                top->node = new_node;

                /* each subtree gets a zero-based sequence */
                top->next_key = 0;
            }
        }
    }
    if (AVSTOR_OK != (res = avstor_commit(db, 1))) {
        printf("%sERROR: avstor_commit failed with %i%s\n", YEL, res, CRESET);
        result = 0;
    }
    else {
        if (nodes_expected != total_nodes) {
            result = 0;
            printf("%sERROR: Total nodes created not equal expected number of nodes.%s\n", YEL, CRESET);
        }
        else {
            result = 1;
        }
    }
close_and_return:
    /* destroy nodes in stack (in case of error) */
    while (level >= 0) {
        avstor_node_destroy(&st[level--].node);
    }
    free(st);
    avstor_close(db);
    return result;
}

#define TEST_DB "test.db"

static const long NODECOUNT_LIST[] = { 100, 100, 100 };
static const struct dfs_create_db_param
DFS_CREATE_DB_PARAM = { TEST_DB, 4096, 3, (long*)&NODECOUNT_LIST };

DEFINE_TEST_LIST(DFS) {
    { "Create DB for DFS", &dfs_create_db, 0, (void*)&DFS_CREATE_DB_PARAM }
};

DEFINE_TESTS(DFS);
