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
#include <stdlib.h>
#include <stdio.h>
#include <avstor.h>

#include "avsdb.h"

#define AVSCRDB_CACHE_SIZE 4096

static long create_db(const char* filename, int level_count, long *child_count)
{
    avstor_key key;

    /* depth-first stack and pointer to top */
    struct st_elem {
        avstor_node node;
        int32_t     next_key;
    } *st, *top;
    avstor *db;
    long total_nodes = 0;
    int res, level = 0;

    /* allocate stack for a depth-first insertion algorithm */
    st = calloc(level_count, sizeof(struct st_elem));
    if (!st) {
        fprintf(stderr, "create_db: calloc failed\n");
        return -1;
    }

    /* create (or overwrite) a new database file */
    if (AVSTOR_OK != (res = avstor_open(&db, filename, AVSCRDB_CACHE_SIZE,
                                        AVSTOR_OPEN_CREATE | AVSTOR_OPEN_READWRITE | AVSTOR_OPEN_AUTOSAVE))) {
        fprintf(stderr, "create_db: avstor_open failed with %i\n", res);
        free(st);
        return -1;
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
        if (top->next_key == child_count[level]) {
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
            p_new_node = (level < (level_count - 1)) ? &new_node : NULL;

            if (AVSTOR_OK != (res = avstor_create_key(&top->node, &key, p_new_node))) {
                fprintf(stderr, "create_db: avstor_create_key failed with %i\n", res);
                if (p_new_node && (res == AVSTOR_EXISTS)) {
                    /* new_node is valid if we get this error, so destroy */
                    avstor_node_destroy(p_new_node);
                }
                total_nodes = -1;
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
    avstor_commit(db, 1);
close_and_return:
    /* destroy nodes in stack (in case of error) */
    while (level >= 0) {
        avstor_node_destroy(&st[level--].node);
    }
    free(st);
    avstor_close(db);    
    return total_nodes;
}

static void show_copyright(void)
{
    printf("libavstor Test Database Creation Utility\n"
           "BSD 3-Clause License\n"
           "Copyright (c) 2025 Tamas Fejerpataky\n"
           "See project at https://github.com/obseedian2024/libavstor\n\n");
}

static void show_help(void)
{
    printf("Usage: avscrdb <filename> # [#...]\n"
           "\twhere # [#...] is a list of space-separated integers specifying the\n"
           "\tnumber of keys in each subtree of the level, with the top level\n"
           "\tbeing mandatory.\n\n"
           "Example: avcrdb test.db 100 50 200\n"
           "\twill create a file called test.db with a hierarchy of 3 levels,\n"
           "\t100 nodes in the first level, each of those nodes having 50\n"
           "\tchildren each, and each of those having 200 children.\n");
}

int main(int argc, char *argv[])
{
    char *filename;
    long *levels, nodes_created, nodes_expected, nodes_per_level;
    int num_levels, i;

    show_copyright();

    if (argc < 3) {
        show_help();
        return 0;
    }
    filename = argv[1];
    num_levels = argc - 2;

    if (NULL == (levels = calloc(num_levels, sizeof(*levels)))) {
        fprintf(stderr, "calloc failed.\n");
        exit(1);
    }

    /* calculate expected number of nodes to be created while parsing arguments */
    nodes_expected = 0;
    nodes_per_level = 1;
    for (i = 0; i < num_levels; i++) {
        if ((levels[i] = strtol(argv[i + 2], NULL, 10)) <= 0) {
            fprintf(stderr, "Invalid argument.\n");
            free(levels);
            exit(1);
        }
        nodes_per_level *= levels[i];
        nodes_expected += nodes_per_level;
    }
    printf("Number of nodes to be inserted: %li\n", nodes_expected);

    printf("Creating file...\n");

    nodes_created = create_db(filename, num_levels, levels);
    if (nodes_created >= 0) {
        printf("Total number of nodes inserted: %li\n", nodes_created);
        if (nodes_created != nodes_expected) {
            /* should not happen if everything is working */
            printf("WARNING: Expected number of nodes not equal to nodes actually written.\n");
        }
    }
    free(levels);
    return 0;
}
