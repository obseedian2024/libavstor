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

#ifndef AVSTOR_H
#define AVSTOR_H

#include <stddef.h>
#include <stdint.h>

#if defined(_WINDLL)
#if defined(AVCALL)
#undef AVCALL
#endif
#define AVCALL __stdcall
#elif !defined(AVCALL)
#define AVCALL
#endif

#define AVSTOR_AVL_HEIGHT       64  // Maximum AVL tree height

// Node types
#define AVSTOR_TYPE_KEY         0
#define AVSTOR_TYPE_INT32       1
#define AVSTOR_TYPE_INT64       2
#define AVSTOR_TYPE_DOUBLE      3
#define AVSTOR_TYPE_STRING      4   // Null terminated but terminating NULL is not stored
#define AVSTOR_TYPE_BINARY      5
#define AVSTOR_TYPE_LONGSTRING  6   // Null terminated but terminating NULL is not stored
#define AVSTOR_TYPE_LONGBINARY  7
#define AVSTOR_TYPE_LINK        8

#define AVSTOR_KEYS             0
#define AVSTOR_VALUES           1
#define AVSTOR_ASCENDING        0
#define AVSTOR_DESCENDING       2

#define AVSTOR_INVALID_HANDLE   (-1)

#ifdef __cplusplus
extern "C" {
#endif

enum {
    AVSTOR_OK = 0,      // Success
    AVSTOR_PARAM,       // A supplied parameter to a function is invalid
    AVSTOR_MISMATCH,    // A function was expecting a different node type
    AVSTOR_NOMEM,       // Memory allocation failed
    AVSTOR_NOTFOUND,    // Node was not found
    AVSTOR_EXISTS,      // Node exists
    AVSTOR_IOERR,       // IO error
    AVSTOR_CORRUPT,     // Data file corruption detected
    AVSTOR_INVOPER,     // Invalid operation
    AVSTOR_INTERNAL,    // Internal error
    AVSTOR_ABORT        // Operation aborted
};

enum {
    AVSTOR_FILE_64BIT       = 0x00000001,
    AVSTOR_FILE_BIGENDIAN   = 0x00000002
};

enum {
    AVSTOR_OPEN_READWRITE   = 0x00000001,
    AVSTOR_OPEN_READONLY    = 0x00000002,
    AVSTOR_OPEN_CREATE      = 0x00000004,
    AVSTOR_OPEN_SHARED      = 0x00000008,
    AVSTOR_OPEN_AUTOSAVE    = 0x00000100
};

typedef struct avstor   avstor;

// Node references in the file are linear offsets from file start.
// Define AVSTOR_CONFIG_FILE_64BIT to use 64-bit offsets and enable files larger than 2GB.
#if defined(AVSTOR_CONFIG_FILE_64BIT)
typedef uint64_t        avstor_off;
#else 
typedef uint32_t        avstor_off;
#endif

// This definition should be treated as opaque
typedef struct avstor_node {
    avstor_off          ref;
    avstor              *db;
} avstor_node;

// Stack for inorder traversal
typedef struct avstor_inorder {
    avstor_off          ref[AVSTOR_AVL_HEIGHT];
    avstor              *db;
    int                 top;
    int                 flags;
} avstor_inorder;

typedef struct avstor_key {
    void                *buf;
    size_t              len;    
    int                 (*comparer)(const void *, const void*);
} avstor_key;

int AVCALL avstor_open(avstor **db, const char* filename, unsigned szcache, int oflags);

int AVCALL avstor_close(avstor *db);

int AVCALL avstor_commit(avstor *db, int flush);

int AVCALL avstor_node_init(avstor *db, avstor_node *node);

void AVCALL avstor_node_destroy(avstor_node *node);

int AVCALL avstor_find(const avstor_node *parent, const avstor_key *key,
                       int flags, avstor_node *out_key);

int AVCALL avstor_create_key(const avstor_node *parent, const avstor_key *key, 
                             avstor_node *out_key);

int AVCALL avstor_create_string(const avstor_node *parent, const avstor_key *key,
                                const char *value, avstor_node *out_value);

int AVCALL avstor_create_binary(const avstor_node *parent, const avstor_key *key, const void *value,
                                size_t szvalue, avstor_node *out_value);

int AVCALL avstor_create_int32(const avstor_node *parent, const avstor_key *key,
                               int32_t value, avstor_node *out_value);

int AVCALL avstor_create_int64(const avstor_node *parent, const avstor_key *key,
                               int64_t value, avstor_node *out_value);

int AVCALL avstor_create_double(const avstor_node *parent, const avstor_key *key,
                                double value, avstor_node *out_value);

int AVCALL avstor_create_link(const avstor_node *parent, const avstor_key *key,
                              const avstor_node *target, avstor_node *out_value);

int AVCALL avstor_get_name(const avstor_node *node, avstor_key *key);

int AVCALL avstor_get_value(const avstor_node* value, void *buf, size_t szbuf, 
                            unsigned *out_type, size_t *out_bytes, uint32_t *out_length);

int AVCALL avstor_get_int32(const avstor_node *value, int32_t *out_val);

int AVCALL avstor_get_int64(const avstor_node *value, int64_t *out_val);

int AVCALL avstor_get_double(const avstor_node *value, double *out_val);

int AVCALL avstor_get_string(const avstor_node* value, char* buf,
                             size_t szbuf, uint32_t *out_length);

int AVCALL avstor_get_binary(const avstor_node* value, void* buf,
                             size_t szbuf, size_t *out_bytes, uint32_t *out_length);

int AVCALL avstor_get_link(const avstor_node *value, avstor_node *out_target);

int AVCALL avstor_get_type(const avstor_node* value, unsigned *out_type);

int AVCALL avstor_update_int32(const avstor_node *value, int32_t new_val);

int AVCALL avstor_update_int64(const avstor_node *value, int64_t new_val);

int AVCALL avstor_update_double(const avstor_node *value, double new_val);

int AVCALL avstor_update_string(const avstor_node* value, const char* new_value);

int AVCALL avstor_update_binary(const avstor_node* value, const void* buf, size_t szbuf);

int AVCALL avstor_delete(const avstor_node *parent, int flags, const avstor_key *key);

int AVCALL avstor_inorder_first(avstor_inorder *st, const avstor_node *parent, const avstor_key *key,
                                int flags, avstor_node *out_node);

int AVCALL avstor_inorder_next(avstor_inorder *st, avstor_node *out_node);

const char* AVCALL avstor_get_errstr(void);

int AVCALL avs_check_cache_consistency(avstor *db);

#ifdef __cplusplus
}
#endif

#endif //AVSTOR_H
