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
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <setjmp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

#include <avstor.h>

#define PAGE_SIZE               4096
#define L2_ASSOC                8
#if defined(__I86__)
#define DEFAULT_BLOCK_SIZE      32u
#else
#define DEFAULT_BLOCK_SIZE      64u
#endif

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN 1
#include <Windows.h>
#endif

#if defined(__unix__)
#include <unistd.h>
#define stricmp strcasecmp
#else
#include <io.h>
#endif

#if defined(AVSTOR_CONFIG_THREAD_SAFE)
#if (defined(__STDC_VERSION__) && (__STDC_VERSION__ >=201112L)) || defined(AVSTOR_CONFIG_FORCE_C11_THREADS)
#include <stdatomic.h>
#include <threads.h>
#define USE_C11_THREADS 1
 
#if defined(_WIN32) && (_WIN32_WINNT >= 0x0600) && !defined(AVSTOR_CONFIG_FORCE_C11_THREADS)
// Under Vista+ use Win32 API SRW locks and condition variables instead of the much slower C11 
// implementation. We don't need the recursion and timeout features anyway. 
#define USE_WIN32_SRW_LOCKS 1
#endif
#else 
#error Concurrency not supported on this platform.
#endif
#endif 

#if !defined(offsetof)
#define offsetof(t, d)          ((size_t)&((t*)(0))->d)
#endif

#if (defined(_WIN32) && !defined(_WINDLL)) || defined(__unix__) || defined(__OS2__)
#if defined(__WATCOMC__) || defined(_MSC_VER)
#define THREAD_LOCAL __declspec(thread)
#elif defined(__clang__) || defined(__GNUC__)
#define THREAD_LOCAL __thread
#else 
#define THREAD_LOCAL
#error "Define THREAD_LOCAL for compiler"
#endif
#elif defined(__DOS__) || defined(_DOS) || defined(MSDOS)
#define THREAD_LOCAL
#elif !defined(_WINDLL)
#define THREAD_LOCAL
#error "Define THREAD_LOCAL for platform"
#endif

// Workaround for IntelliSense Clang bug (https://github.com/microsoft/vscode-cpptools/issues/11585)
//#if defined(__clang__) && defined(_WIN32) && defined(UINT32_MAX)
//#undef UINT32_MAX
//#define UINT32_MAX ((uint32_t)~(uint32_t)0)
//#endif

#if defined(__DOS__) || defined(_DOS) || defined(MSDOS)
#if !defined(__DOS__)
#define __DOS__ 1
#endif
#if defined(__I86__) || defined(M_I86) || defined(_M_I86)
#define DOS16 1
#else
#define DOS32 1
#endif
#elif defined(__OS2__) 
#if defined(__I86__) || defined(M_I86) || defined(_M_I86)
#define OS2_16 1
#else
#define OS2_32 1
#endif
#endif

#if defined(__I86__) || defined(M_I86) || defined(_M_I86) 
#if !defined(__I86__)
#define __I86__ 1
#endif
#endif

#if defined(AVSTOR_CONFIG_THREAD_SAFE) && !defined(_WIN32) && !defined(__unix__)
#define IO_REQUIRES_SYNC 1
#endif

// For DOS and Windows platforms
#if !defined(O_BINARY)
#define O_BINARY 0x0
#endif

// turn off nannying
#ifdef _MSC_VER
#pragma warning(disable:4996) // deprecated
#if defined(NDEBUG)
#pragma warning(disable:4100) // unreferenced parameter
#endif
#endif

#if (defined(__clang__) || defined(__GNUC__))
#define NORETURN __attribute__((noreturn))
#elif defined(__WATCOMC__) || (defined(_MSC_VER) && _MSC_VER >= 1200)
#define NORETURN __declspec(noreturn)
#else
#define NORETURN
#endif

#if defined(_WINDLL)
#if (defined(__clang__) || defined(__GNUC__))
#define FALLTHROUGH __attribute__((fallthrough))
#else 
#define FALLTHROUGH
#endif
#endif

#define PTR_DIFF(base, addr)    (unsigned)((char*)(addr) - (char*)(base))
#define PTR(base, ofs)          ((void*)&(((char*)(base))[ofs]))
#define CONST_PTR(base, ofs)    ((const void*)&(((const char*)(base))[ofs]))

#define PAGES_PER_BLOCK         (DEFAULT_BLOCK_SIZE * 1024 / PAGE_SIZE)

#if !defined(AVSTOR_CONFIG_FILE_64BIT) || defined(__DOS__)
#define MAX_FILE_PAGES          (0x80000000U / (unsigned)PAGE_SIZE - 1U)
#elif defined(AVSTOR_CONFIG_FILE_64BIT)
#define MAX_FILE_PAGES          0x0FFFFFFFFU
#endif
#define INVALID_INDEX           0
#define PAGE_HDR                0x00u
#define PAGE_KEYS               0x01u
#define PAGE_DIRTY              0x80u
#define NODE_TYPEMASK           (0x0Fu << 2u)
#define NODE_SIZEMASK           0xFFC0u
#define BF_MASK                 0x03u
#define NODE_FLAG_VAR           1
#define NODE_FLAG_LONGVAR       2
#define MAX_KEY_LEN             240u
#define MAX_BINARY_LEN          250u
#define MAX_STRING_LEN          250u
#define SIZE_PAGE_HDR           offsetof(AvPage, hdr_end)
#define SIZE_NODE_HDR           offsetof(AvNode, name)
#define PAGE_MASK               (~((uintptr_t)PAGE_SIZE - 1u))
#define OFFSET_MASK             (~((avstor_off)PAGE_SIZE - 1u))
#define KB_PER_PAGE             (PAGE_SIZE / 1024)
#define BF(node)                (((int)((node)->hdr & BF_MASK)) - 1)
#define NODE_TYPE(node)         (((node)->hdr & NODE_TYPEMASK) >> 2)

#define get_node_size(node)     ((unsigned)((node)->hdr & NODE_SIZEMASK) >> 4u)
#define align_node(sz)          (((sz) + 3) & ~0x3u)

#if defined(NDEBUG)
#define RETURN(result, msg)     do { last_err_msg = msg; return (result); } while(0)
#define CHECK_PARAM(cond)       ((void)0)
#else
static void print_error(const char* msg, const char* filename, unsigned linenum)
{
    fprintf(stderr, "libavstor: %s:%u: %s\n", filename, linenum, msg);
}
#define errout(c)  print_error((c),__FILE__, __LINE__)
#define RETURN(result, msg) do { last_err_msg = msg; print_error((msg),__FILE__, __LINE__); return (result); } while(0)
#define CHECK_PARAM(cond)   do { \
                                if (!(cond)) { \
                                    errout("Parameter cannot be null. Process aborted."); \
                                    abort(); \
                                } \
                            } while(0)
#endif

/* Rudimentary exception support */
typedef enum ExceptionState
{
    EX_STATE_IN_TRY,
    EX_STATE_IN_CATCH,
    EX_STATE_IN_FINALLY
} ExceptionState;

typedef struct ExceptionFrame
{
    jmp_buf		            context;
    struct ExceptionFrame   *prev;
    const char  		    *file;
    const char              *msg;
    int					    err;
    ExceptionState          state;
    int					    line_no;
} ExceptionFrame;

#if defined(_WINDLL)
typedef struct AvTLSData {
    ExceptionFrame          *tls_cur_ex;
    const char              *tls_last_err_msg;
} AvTLSData;
#endif

#if defined(AVSTOR_CONFIG_THREAD_SAFE)
#if defined(USE_C11_THREADS)
typedef _Atomic(int32_t)    atomic_int32;
#else 
typedef int32_t             atomic_int32;
#endif

typedef struct AvMutex {
#if defined(USE_WIN32_SRW_LOCKS)   
    SRWLOCK		            mtx;
#elif defined(USE_C11_THREADS)
    mtx_t		            mtx;
#endif
} AvMutex;

typedef struct AvCnd {
#if defined(USE_WIN32_SRW_LOCKS)
    CONDITION_VARIABLE		cv;
#elif defined(USE_C11_THREADS)
    cnd_t                   cv;
#endif
} AvCnd;

// quick & dirty upgradable non-recursive read-write lock using condition variables
typedef struct rwl_t {
    AvMutex		mtx;
    AvCnd		cv;         // Shared and exlusive locks wait on this cv
    AvCnd		cv_upgr;    // Upgrader waits on this cv
    int			lock;
} rwl_t;
#endif

// Since nodes in 64-bit files are also 4-byte aligned, we use this
// struct rather than using int64_t directly to retain 4 byte alignment
typedef struct NodeRef {
#if defined(AVSTOR_CONFIG_FILE_64BIT)
    uint32_t            offset[2];
#else 
    avstor_off          offset;
#endif
} NodeRef;

typedef struct AvPage AvPage;
typedef struct {
    AvPage*             page;
    avstor_off          offset;
    uint32_t            load_time;
} CacheItem;

// Represents page data in the file
struct AvPage {
    // "AVST" for header, "AVDP" for data page
    //char                id[4];

    // page checksum when on file, LRU count in memory
    uint32_t            checksum;

    // lock count 
#if defined(AVSTOR_CONFIG_THREAD_SAFE)
    volatile atomic_int32 lock_count;
#else 
    int32_t             lock_count;
#endif
    // offset of page in the file
    avstor_off            page_offset;
#if !defined(AVSTOR_CONFIG_FILE_64BIT)
    int32_t             pad_offset;
#endif      

    // bit field, PAGE_DIRTY denotes modified pages
    uint8_t             status;

    // PAGE_HDR, PAGE_KEYS
    uint8_t             type;

    uint8_t             reserved[2];

    union {
        // Header page (first page in file), type PAGE_HDR
        struct {
            // Number of pages in file
            uint32_t            pagecount;

            // hardcoded to 4096 for now
            uint32_t            pagesize;

            // root AVL tree node for data
            NodeRef             root;
#if !defined(AVSTOR_CONFIG_FILE_64BIT)
            int32_t             pad_root;
#endif           

            NodeRef             root_links;
#if !defined(AVSTOR_CONFIG_FILE_64BIT)
            int32_t             pad_root_links;
#endif           

            uint32_t            flags;

            // page number of the last page a node was inserted into
            uint32_t            page_pool[256];

            // placeholder for end of hdr
            char                hdr_end;
        };

        // Data page, type PAGE_KEYS
        struct {
            // top of free space in the page.
            uint16_t            top;

            // points to the first free element in index, or INVALID_INDEX if no
            // more free indexes
            uint16_t            index_freelist;

            // current length of the index array
            uint16_t            index_count;

            // array of offsets pointing to the actual node data. Relative to page address.
            // If part of the free list, an offset to the next free index, or
            // INVALID_INDEX if at the end of the list.
            // variable size actually, should be flexible array but to support old compilers
            // it is done this way
            uint16_t            index[2000];
        };
    };
};

typedef struct CacheRow {
#ifdef AVSTOR_CONFIG_THREAD_SAFE
    rwl_t               lock;
#endif
    uint32_t            load_count;
    unsigned            capacity;
    CacheItem*          items;
} CacheRow;

typedef struct PageCache {
    CacheRow*           rows;
    AvPage*             header;
    AvPage*             old_header;
    unsigned            l2_len;
    unsigned            l2_mask;
} PageCache;

typedef struct BufferPool {
#if defined(AVSTOR_CONFIG_THREAD_SAFE)
    AvMutex             lock;
#endif
    AvPage**            blocks;
    unsigned            capacity;
    unsigned            count;
    unsigned            next_page;
} BufferPool;

struct avstor {
#if defined(AVSTOR_CONFIG_THREAD_SAFE)
    rwl_t               global_rwl;
#if defined(IO_REQUIRES_SYNC)
    AvMutex             io_mtx;
#endif
#endif
    int                 file;
    int                 oflags;
    unsigned            l2_size;
    BufferPool          bpool;
    PageCache           cache;
};

typedef struct AvStackData AvStackData;
typedef struct {
    struct AvStackData {
        avstor_off        noderef;
        int             comp;
    }                   data[AVSTOR_AVL_HEIGHT];
    int                 top;
    NodeRef*            root;
} AvStack;

typedef struct AvNode {
    uint16_t            hdr;
    uint16_t            index;
    NodeRef             left;
    NodeRef             right;
    uint8_t             szname;
    char                name[];
} AvNode;

// Key node
struct AvKey {
    NodeRef             subkey_root;
    NodeRef             value_root;
    uint16_t            level;
    uint8_t             pad[2];
};

// int32 node
struct AvInt32Value {
    int32_t             value;
};

typedef union AvValue64 {
    double              as_dbl;
    int64_t             as_int64;
} AvValue64;

// int64 or double node, aligned to 4 bytes
struct AvInt64Value {
    struct {
        uint32_t  dw[2];
    }                   value;
};

// variable length string or binary node
struct AvVarValue {
    uint8_t             length;
};

// long string or binary node
struct AvLVarValue {
    uint32_t            length;
    NodeRef             root;
};

struct AvLinkValue {
    NodeRef             link;
};

// Fixed data portion of node
typedef union AvNodeData {
    struct AvKey            vkey;
    struct AvInt32Value     v32;
    struct AvInt64Value     v64;
    struct AvVarValue       vvar;
    struct AvLVarValue      vlongvar;
    struct AvLinkValue      vLink;
} AvNodeData;

typedef struct AvNodeClass {
    unsigned            szdata;
    unsigned            flags;
} AvNodeClass;

static const char* MSG_INVALID_PARAMETER            = "Invalid parameter";
static const char* MSG_INVALID_FLAGS_COMBINATION    = "Invalid flags combination";
static const char* MSG_NODE_EXISTS                  = "Node with specified name already exists";
static const char* MSG_NO_SPACE_IN_PAGE             = "Not enough free space in page";
static const char* MSG_PAGE_CORRUPTED               = "Page corrupted";
static const char* MSG_TYPE_MISMATCH                = "Node type mismatch";
static const char* MSG_OUT_OF_MEMORY                = "Out of memory";
static const char* MSG_BACKTRACE_OVERFLOW           = "Backtrace stack overflow";
static const char* MSG_BACKTRACE_UNDERFLOW          = "Backtrace stack underflow";
static const char* MSG_INVALID_ATTRIBUTE            = "Invalid attribute";

static const char* err_codes[] =
{
    "AVSTOR_OK",
    "AVSTOR_PARAM",
    "AVSTOR_MISMATCH",
    "AVSTOR_NOMEM",
    "AVSTOR_NOTFOUND",
    "AVSTOR_EXISTS",
    "AVSTOR_IOERR",
    "AVSTOR_CORRUPT",
    "AVSTOR_INVOPER",
    "AVSTOR_INTERNAL",
    "AVSTOR_ABORT"
};

#if defined(AVSTOR_CONFIG_FILE_64BIT)
static const NodeRef    NODEREF_NULL = { { 0, 0 } };
#else 
static const NodeRef    NODEREF_NULL = { 0 };
#endif

static const AvNodeClass NODE_CLASS[16] = {
    { sizeof(struct AvKey), 0 },                        // 0x00  (NODE_KEY)
    { sizeof(struct AvInt32Value), 0 },                 // 0x01  (NODE_INT32)
    { sizeof(struct AvInt64Value), 0 },                 // 0x02  (NODE_INT64)
    { sizeof(struct AvInt64Value), 0 },                 // 0x03  (NODE_DOUBLE)
    { sizeof(struct AvVarValue), NODE_FLAG_VAR },       // 0x04  (NODE_STRING)
    { sizeof(struct AvVarValue), NODE_FLAG_VAR },       // 0x05  (NODE_BINARY)
    { sizeof(struct AvLVarValue), NODE_FLAG_LONGVAR },  // 0x06  (NODE_LONGSTRING)
    { sizeof(struct AvLVarValue), NODE_FLAG_LONGVAR },  // 0x07  (NODE_LONGBINARY)
    { sizeof(struct AvLinkValue), 0 },                  // 0x08  (NODE_LINK)
    { 0, 0 },                                           // 0x09  (unused)
    { 0, 0 },                                           // 0x0A  (unused)
    { 0, 0 },                                           // 0x0B  (unused)
    { 0, 0 },                                           // 0x0C  (unused)
    { 0, 0 },                                           // 0x0D  (unused)
    { 0, 0 },                                           // 0x0E  (unused)
    { 0, 0 }                                            // 0x0F  (unused)
};

#if defined(_WINDLL)
static DWORD tls_idx;

#define cur_ex              ((AvTLSData*)TlsGetValue(tls_idx))->tls_cur_ex
#define last_err_msg        ((AvTLSData*)TlsGetValue(tls_idx))->tls_last_err_msg

#else
static
THREAD_LOCAL
ExceptionFrame *cur_ex = NULL;

static
THREAD_LOCAL
const char* last_err_msg = NULL;
#endif

#define is_invalid_avstor_key(key)   ((key)->len > MAX_KEY_LEN)

NORETURN
static void throw_ex(int err, const char* msg, int line_no, const char* file, struct ExceptionFrame *ex_orig)
{
    while (cur_ex && cur_ex->state != EX_STATE_IN_TRY) {
        if (cur_ex->state == EX_STATE_IN_CATCH) {
            fprintf(stderr, "libavstor: Attempting to throw exception %s from catch handler at line %i in %s.\n"
                    , err_codes[err], line_no, file);
            fprintf(stderr, "--> Original exception %s at line %i in %s. Process aborted.\n"
                    , err_codes[cur_ex->err], cur_ex->line_no, cur_ex->file);
            exit(1);
        }
        else if (cur_ex->state == EX_STATE_IN_FINALLY) {
            /* pop current handler. */
            cur_ex = cur_ex->prev;
        }
    }
    if (cur_ex) {
        if (!ex_orig) {
            cur_ex->line_no = line_no;
            cur_ex->file = file;
            cur_ex->err = err;
            cur_ex->msg = msg;
            last_err_msg = msg;
#if !defined(NDEBUG)
            fprintf(stderr, "libavstor: Exception %s: %s\n", err_codes[err], msg);
            fprintf(stderr, "  at line %i in %s\n", line_no, file);
#endif
        }
        else {
            cur_ex->line_no = ex_orig->line_no;
            cur_ex->file = ex_orig->file;
            cur_ex->err = ex_orig->err;
            cur_ex->msg = ex_orig->msg;
        }
        longjmp(cur_ex->context, err);
    }
    else {
        fprintf(stderr, "libavstor: Unhandled exception %s at line %i in %s. Process aborted.\n", err_codes[err]
                , line_no, file);
        exit(1);
    }
}

/***************************************** WARNING! ***********************************************/
/* Locals that are modified in TRY block and referenced in CATCH block must be declared volatile! */
/**************************************************************************************************/
#define TRY(ex)         do { ExceptionFrame ex;             \
                            ex.err = 0;                     \
                            ex.state = EX_STATE_IN_TRY;     \
	                        ex.prev = cur_ex;               \
	                        cur_ex = &ex;                   \
	                    if (0 == setjmp(ex.context)) {

#define END_TRY(ex)     }                                                           \
                        if (ex.state == EX_STATE_IN_CATCH) ex.err = 0;              \
                        cur_ex = ex.prev;                                           \
		                if (ex.err) throw_ex(0, NULL, __LINE__, __FILE__, &ex);  \
                        } while(0)

#define CATCH_ANY(ex)   } else {                                \
                            ex.state = EX_STATE_IN_CATCH;

/* Currently not used but could be
#define CATCH(ex, e)    } else if (ex.err == (e)) {             \
                            ex.state = EX_STATE_IN_CATCH;
*/
#define FINALLY(ex)     } \
                        { \
                            if (ex.state == EX_STATE_IN_CATCH) ex.err = 0; \
                            ex.state = EX_STATE_IN_FINALLY; 

#define THROW(err_no, msg)   throw_ex((err_no), (msg), __LINE__, __FILE__, NULL)

#if defined(AVSTOR_CONFIG_THREAD_SAFE)
#if defined(USE_C11_THREADS)
#define atomic_inc_int32(addend)                atomic_fetch_add((addend), 1)
#define atomic_dec_int32(addend)                atomic_fetch_add((addend), -1)
#define atomic_load_int32_relaxed(x)            atomic_load_explicit((x), memory_order_relaxed)
#define atomic_store_int32_release(x, value)    atomic_store_explicit((x), (value), memory_order_release)

#elif defined(_WIN32)

#define atomic_inc_int32(addend) ((atomic_int32)InterlockedIncrement((volatile LONG*)(addend)) - 1)
#define atomic_dec_int32(addend) ((atomic_int32)InterlockedDecrement((volatile LONG*)(addend)) + 1)

static __inline atomic_int32 atomic_load_int32_relaxed__(volatile atomic_int32 *x)
{
    return *x;
}

static __inline void atomic_store_int32_release__(volatile atomic_int32 *x, atomic_int32 value)
{
    (void)InterlockedExchange((volatile LONG*)x, value);
}

#define atomic_load_int32_relaxed(x)            atomic_load_int32_relaxed__(x)
#define atomic_store_int32_release(x, value)    atomic_store_int32_release__(x, value)
#else 
#error Atomics not supported on this platform
#endif

#if defined(USE_WIN32_SRW_LOCKS)
static __inline int avmtx_init(AvMutex* avmtx)
{
    InitializeSRWLock(&avmtx->mtx);
    return 1;
}

static __inline void avmtx_destroy(AvMutex* avmtx)
{
    memset(avmtx, 0, sizeof(*avmtx));
}

static __inline void avmtx_lock(AvMutex* avmtx)
{
    AcquireSRWLockExclusive(&avmtx->mtx);
}

static __inline void avmtx_unlock(AvMutex* avmtx)
{
    ReleaseSRWLockExclusive(&avmtx->mtx);
}

static __inline int avcnd_init(AvCnd* avcv)
{
    InitializeConditionVariable(&avcv->cv);
    return 1;
}

static __inline void avcnd_destroy(AvCnd* avcv)
{
    memset(avcv, 0, sizeof(*avcv));
}

static __inline int avcnd_wait(AvCnd* avcv, AvMutex* avmtx)
{
    return SleepConditionVariableSRW(&avcv->cv, &avmtx->mtx, INFINITE, 0) == TRUE;
}

static __inline int avcnd_signal(AvCnd* avcv)
{
    WakeConditionVariable(&avcv->cv);
    return 1;
}

static __inline int avcnd_broadcast(AvCnd* avcv)
{
    WakeAllConditionVariable(&avcv->cv);
    return 1;
}
#elif defined(USE_C11_THREADS) 
static __inline int avmtx_init(AvMutex* avmtx)
{
    return mtx_init(&avmtx->mtx, mtx_plain) == thrd_success;
}

static __inline void avmtx_destroy(AvMutex* avmtx)
{
    mtx_destroy(&avmtx->mtx);
    memset(avmtx, 0, sizeof(*avmtx));
}

static __inline void avmtx_lock(AvMutex* avmtx)
{
    mtx_lock(&avmtx->mtx);
}

static __inline void avmtx_unlock(AvMutex* avmtx)
{
    mtx_unlock(&avmtx->mtx);
}

static __inline int avcnd_init(AvCnd* avcv)
{
    return cnd_init(&avcv->cv) == thrd_success;
}

static __inline void avcnd_destroy(AvCnd* avcv)
{
    cnd_destroy(&avcv->cv);
    memset(avcv, 0, sizeof(*avcv));
}

static __inline int avcnd_wait(AvCnd* avcv, AvMutex* avmtx)
{
    return cnd_wait(&avcv->cv, &avmtx->mtx) == thrd_success;
}

static __inline int avcnd_signal(AvCnd* avcv)
{
    return cnd_signal(&avcv->cv) == thrd_success;
}

static __inline int avcnd_broadcast(AvCnd* avcv)
{
    return cnd_broadcast(&avcv->cv) == thrd_success;
}
#endif
static int rwl_init(rwl_t *rwl)
{
    rwl->lock = 0;
    if (avcnd_init(&rwl->cv)) {
        if (!avcnd_init(&rwl->cv_upgr)) {
            goto err_cv_upgr;
        }
        if (!avmtx_init(&rwl->mtx)) {
            goto err_mtx;
        }
        return 1;
err_mtx:
        avcnd_destroy(&rwl->cv_upgr);
err_cv_upgr:
        avcnd_destroy(&rwl->cv);
    }
    return 0;
}

static void rwl_destroy(rwl_t *rwl)
{
    avcnd_destroy(&rwl->cv);
    avcnd_destroy(&rwl->cv_upgr);
    avmtx_destroy(&rwl->mtx);
    rwl->lock = 0;
}

static void rwl_lock_shared(rwl_t *rwl)
{
    avmtx_lock(&rwl->mtx);
    while (rwl->lock < 0 || (rwl->lock & 1)) {
        avcnd_wait(&rwl->cv, &rwl->mtx);
    }
    rwl->lock += 2;
    avmtx_unlock(&rwl->mtx);
}

static void rwl_lock_exclusive(rwl_t *rwl)
{
    avmtx_lock(&rwl->mtx);
    while (rwl->lock != 0) {
        avcnd_wait(&rwl->cv, &rwl->mtx);
    }
    rwl->lock = -2;
    avmtx_unlock(&rwl->mtx);
}

static int rwl_upgrade(rwl_t *rwl)
{
    int lock;
    int result = 0;
    avmtx_lock(&rwl->mtx);
    if (!(rwl->lock & 1)) {
        lock = rwl->lock & ~1;
        while (lock != 2 && lock > 0) {
            rwl->lock |= 1;
            avcnd_wait(&rwl->cv_upgr, &rwl->mtx);
            lock = rwl->lock & ~1;
        }
        rwl->lock = (lock == 2) ? -2 : lock;
        result = 1;
    }
    avmtx_unlock(&rwl->mtx);
    return result;
}

static void rwl_release(rwl_t *rwl)
{
    int lock;
    avmtx_lock(&rwl->mtx);
    lock = rwl->lock;
    if (lock > 1) {
        rwl->lock = lock - 2;
    }
    else if ((lock & ~1) <= -2) {
        rwl->lock = lock + 2;
    }
    else {
        goto unlock_and_exit;
    }

    if (rwl->lock == 3) {
        avcnd_signal(&rwl->cv_upgr);
    }
    else if ((lock & ~1) == -2 && (rwl->lock & ~1) == 0) {
        avmtx_unlock(&rwl->mtx);
        avcnd_broadcast(&rwl->cv);
        return;
    }
unlock_and_exit:
    avmtx_unlock(&rwl->mtx);
}

static int rwl_upgrade_or_lock_exclusive(rwl_t *rwl)
{
    if (rwl_upgrade(rwl)) {
        return 1;
    }
    rwl_release(rwl);
    rwl_lock_exclusive(rwl);
    return 0;
}

static int rwl_upgrade_or_release(rwl_t *rwl)
{
    if (rwl_upgrade(rwl)) {
        return 1;
    }
    rwl_release(rwl);
    return 0;
}
#else 
#define rwl_init(rwl) (1)
#define rwl_destroy(rwl) ((void)0)
#define rwl_lock_shared(rwl) ((void)0)
#define rwl_lock_exclusive(rwl) ((void)0)
#define rwl_release(rwl) ((void)0)
#define rwl_upgrade_or_lock_exclusive(rwl) (1)
#define rwl_upgrade_or_release(rwl) (1)
#define avmtx_init(mtx) (1)
#define avmtx_destroy(mtx) ((void)0)
#define avmtx_lock(mtx) ((void)0)
#define avmtx_unlock(mtx) ((void)0)
#define atomic_load_int32_relaxed(v) (*(v))

static __inline void atomic_store_int32_release(int32_t *dest, int32_t value)
{
    *dest = value;
}
#endif

#if defined(AVSTOR_CONFIG_FILE_64BIT)
static __inline avstor_off nref_to_ofs(const NodeRef ref)
{
    union {
        NodeRef  ref;
        avstor_off offset;
    } result;
    result.ref = ref;
    return result.offset;
}

static __inline NodeRef ofs_to_nref(const avstor_off ofs)
{
    union {
        NodeRef  ref;
        avstor_off offset;
    } result;
    result.offset = ofs;
    return result.ref;
}

#define is_nref_empty(n) (nref_to_ofs(n) == 0)
#else 
static __inline NodeRef ofs_to_nref(const avstor_off ofs)
{
    NodeRef result;
    result.offset = ofs;
    return result;
}

#define nref_to_ofs(ref) (ref).offset
#define is_nref_empty(n) ((n).offset == 0)
#endif

//static const char               FILE_ID[4] = "AVST";
//static const char               PAGE_ID[4] = "AVDP";

/*
* Aligned allocation functions
* Based on malloc & free.
*/
static void* avs_aligned_malloc(size_t size, size_t alignment)
{
    void *ptr, *res;
    assert(alignment >= sizeof(uintptr_t));
    if (!(ptr = malloc(size + alignment))) {
        return ptr;
    }

    //align pointer
    res = (void*)(((uintptr_t)ptr + alignment) & ~((uintptr_t)alignment - 1));

    //store malloc pointer for deallocation
    assert(PTR_DIFF(ptr, res) >= sizeof(uintptr_t));
    ((uintptr_t*)res)[-1] = (uintptr_t)ptr;
    return res;
}

static void avs_aligned_free(void *memblock)
{
    free((void*)((uintptr_t*)memblock)[-1]);
}

static int bpool_init(BufferPool *bp, unsigned initial_capacity)
{
    if (!avmtx_init(&bp->lock)) {
        return 0;
    }
    if (!(bp->blocks = calloc((size_t)initial_capacity, sizeof(*bp->blocks)))) {
        goto err_calloc;
    }
    if (!(bp->blocks[0] = avs_aligned_malloc(DEFAULT_BLOCK_SIZE * 1024, PAGE_SIZE))) {
        goto err_aligned_malloc;
    }
    bp->count = 1;
    bp->next_page = 0;
    bp->capacity = initial_capacity;
    return 1;
err_aligned_malloc:
    free(bp->blocks);
    bp->blocks = NULL;
err_calloc:
    avmtx_destroy(&bp->lock);
    return 0;
}

static void bpool_destroy(BufferPool *bp)
{
    unsigned i;
    avmtx_lock(&bp->lock);
    if (bp->blocks) {
        for (i = 0; i < bp->count; ++i) {
            if (bp->blocks[i]) {
                avs_aligned_free(bp->blocks[i]);
                bp->blocks[i] = NULL;
            }
        }
        free(bp->blocks);
        bp->blocks = NULL;
    }
    bp->count = 0;
    bp->next_page = 0;
    bp->capacity = 0;
    avmtx_unlock(&bp->lock);
    avmtx_destroy(&bp->lock);
}

static AvPage* bpool_alloc_page(BufferPool *bp)
{
    AvPage *result = NULL;
    avmtx_lock(&bp->lock);
    if (bp->next_page >= PAGES_PER_BLOCK) {
        if (bp->count >= bp->capacity) {
            AvPage **new_blocks;
            bp->capacity *= 2;
            if (!(new_blocks = realloc(bp->blocks, bp->capacity * sizeof(*bp->blocks)))) {
                bp->capacity /= 2;
                goto err_alloc;
            }
            bp->blocks = new_blocks;
        }
        if (!(bp->blocks[bp->count] = avs_aligned_malloc(DEFAULT_BLOCK_SIZE * 1024, PAGE_SIZE))) {
            goto err_alloc;
        }
        bp->count++;
        bp->next_page = 0;
    }
    result = PTR(bp->blocks[bp->count - 1], bp->next_page++ * PAGE_SIZE);
err_alloc:
    avmtx_unlock(&bp->lock);
    return result;
}

// File IO routines
#if defined(_WIN32)
static int io_open(const char* filename, int oflags)
{
    // Note: 64-bit Windows only uses the lower 32-bits for the handle so it is safe to cast to int
    return (int)(intptr_t)(HANDLE)
        CreateFileA(filename,
                    (oflags & AVSTOR_OPEN_READONLY) ? GENERIC_READ : (GENERIC_READ | GENERIC_WRITE),
                    (oflags & AVSTOR_OPEN_SHARED) ? FILE_SHARE_READ : 0,
                    NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS, NULL);
}

static int io_create(const char* filename, int oflags)
{
    return (int)(intptr_t)(HANDLE)
        CreateFileA(filename, GENERIC_READ | GENERIC_WRITE,
                    (oflags & AVSTOR_OPEN_SHARED) ? FILE_SHARE_READ : 0,
                    NULL, CREATE_ALWAYS, FILE_ATTRIBUTE_NORMAL | FILE_FLAG_RANDOM_ACCESS, NULL);
}

static int io_close(int fid)
{
    return CloseHandle((HANDLE)(intptr_t)fid);
}

static int io_commit(int fid)
{
    return FlushFileBuffers((HANDLE)(intptr_t)fid);
}

static int io_read(avstor *db, void *buf, avstor_off pos, unsigned count)
{
    OVERLAPPED ovlp;
    DWORD bytes;
    ZeroMemory(&ovlp, sizeof(ovlp));
    ovlp.Offset = (DWORD)pos;
#if defined(AVSTOR_CONFIG_FILE_64BIT)
    ovlp.OffsetHigh = pos >> 32;
#endif
    if (!ReadFile((HANDLE)(intptr_t)db->file, buf, count, &bytes, &ovlp)) {
        return GetLastError() == ERROR_HANDLE_EOF ? 0 : -1;
    }
    return (int)bytes;
}

static int io_write(avstor *db, const void *buf, avstor_off pos, unsigned count)
{
    OVERLAPPED ovlp;
    DWORD bytes;
    ZeroMemory(&ovlp, sizeof(ovlp));
    ovlp.Offset = (DWORD)pos;
#if defined(AVSTOR_CONFIG_FILE_64BIT)
    ovlp.OffsetHigh = pos >> 32;
#endif
    if (!WriteFile((HANDLE)(intptr_t)db->file, buf, count, &bytes, &ovlp)) {
        return -1;
    }
    return (int)bytes;
}

//static int io_error(void)
//{
//    return (int)GetLastError();
//}

#else
/* default file io implementation */
static int io_open(const char* filename, int oflags)
{
    int result = open(filename, O_BINARY | (oflags & AVSTOR_OPEN_READONLY ? O_RDONLY : O_RDWR));
    return result < 0 ? AVSTOR_INVALID_HANDLE : result;
}

static int io_create(const char* filename, int oflags)
{
    int result;
    (void)oflags;
    result = open(filename, O_CREAT | O_TRUNC | O_BINARY | O_RDWR, S_IREAD | S_IWRITE);
    return result < 0 ? AVSTOR_INVALID_HANDLE : result;
}

static int io_close(int fid)
{
    return close(fid) >= 0;
}

static int io_commit(int fid)
{
    return fsync((int)fid) >= 0;
}

#if defined(__unix__) 

static int io_read(avstor *db, void *buf, avstor_off pos, unsigned count)
{
    return pread(db->file, buf, count, (off_t)pos);
}

static int io_write(avstor *db, const void *buf, avstor_off pos, unsigned count)
{
    return pwrite(db->file, buf, count, (off_t)pos);
}

#else 
static int io_seek(int fid, avstor_off pos)
{
#if !defined(AVSTOR_CONFIG_FILE_64BIT) || defined(__DOS__)
    return lseek(fid, (long)pos, SEEK_SET) >= 0;
#elif defined(AVSTOR_CONFIG_FILE_64BIT)
#if defined(__FreeBSD__)
    // in recent versions of FreeBSD, off_t is always 64 bits, even for 32-bit executables
    return lseek(fid, (off_t)pos, SEEK_SET) >= 0;
#elif defined(_WIN32) || defined(__WATCOMC__)
    return _lseeki64(fid, (int64_t)pos, SEEK_SET) >= 0;
#elif defined(__linux__)
    return lseek64(fid, (off64_t)pos, SEEK_SET) >= 0;
#else 
#error "64-bit io_seek not implemented for current platform."
#endif
#endif
}

static int io_read(avstor *db, void *buf, avstor_off pos, unsigned count)
{
    int result;
#if defined(IO_REQUIRES_SYNC)
    avmtx_lock(&db->io_mtx);
#endif
    if (!io_seek(db->file, pos)) {
        result = -1;
    }
    else {
        result = read(db->file, buf, count);
    }
#if defined(IO_REQUIRES_SYNC)
    avmtx_unlock(&db->io_mtx);
#endif
    return result;
}

static int io_write(avstor *db, const void *buf, avstor_off pos, unsigned count)
{
    int result;
#if defined(IO_REQUIRES_SYNC)
    avmtx_lock(&db->io_mtx);
#endif
    if (!io_seek(db->file, pos)) {
        result = -1;
    }
    else {
        result = write(db->file, buf, count);
    }
#if defined(IO_REQUIRES_SYNC)
    avmtx_unlock(&db->io_mtx);
#endif
    return result;
}

#endif
//static int io_error(void)
//{
//    return errno;
//}
#endif

static int offset_comparer(const void* v1, const void* v2)
{
    avstor_off ofs1, ofs2;
    memcpy(&ofs1, v1, sizeof(ofs1));
    memcpy(&ofs2, v2, sizeof(ofs2));
    return ofs1 > ofs2 ? 1 : ofs1 < ofs2 ? -1 : 0;
}

// strnlen not supported on all platforms, so here is a replacement
static size_t strlen_l(const char* str, size_t szbuf)
{
    char* pterm = (char*)memchr(str, '\0', szbuf);
    if (!pterm) {
        return szbuf;
    }
    return (size_t)(pterm - str);
}

static __inline void lock_page(AvPage *page)
{
#if defined(AVSTOR_CONFIG_THREAD_SAFE)
#if !defined(NDEBUG)
    int32_t prev =
#endif
        atomic_inc_int32(&page->lock_count);
    assert(prev >= 0);
#else
    assert(page->lock_count >= 0);
    page->lock_count++;
#endif
}

static __inline void unlock_page(AvPage *page)
{
#if defined(AVSTOR_CONFIG_THREAD_SAFE)
#if !defined(NDEBUG)
    int32_t prev =
#endif
        atomic_dec_int32(&page->lock_count);
    assert(prev > 0);
#else
    assert(page->lock_count > 0);
    page->lock_count--;
#endif
}

static __inline void set_page_dirty(AvPage *page)
{
    page->status |= PAGE_DIRTY;
}

static __inline void set_page_clean(AvPage *page)
{
    page->status &= ~PAGE_DIRTY;
}

static __inline int is_page_dirty(AvPage *page)
{
    return (page->status & PAGE_DIRTY) != 0;
}

static const uint32_t MOD_ADLER = 65521;

static uint32_t compute_page_checksum(AvPage *page)
{
    unsigned char *cp = (unsigned char*)page;
    uint32_t a = 1, b = 0;
    unsigned cnt = PAGE_SIZE;

    while (cnt--) {
        a = (a + *cp++);
        b = (b + a);
    }
    a = a % MOD_ADLER;
    b = b % MOD_ADLER;
    return (b << 16) | a;
}

static __inline void update_page_checksum(AvPage *page)
{
    page->checksum = 0;
    page->checksum = compute_page_checksum(page);
}

static void avstor_destroy(avstor *db)
{
    PageCache *cache = &db->cache;
    unsigned i;
    if (cache->rows) {
        for (i = 0; i < cache->l2_len; ++i) {
            if (cache->rows[i].items) {
                free(cache->rows[i].items);
                cache->rows[i].items = NULL;
            }
            rwl_destroy(&cache->rows[i].lock);
        }
        free(cache->rows);
        cache->rows = NULL;
    }
    if (db->cache.header) {
        avs_aligned_free(db->cache.header);
        db->cache.header = NULL;
    }
    db->cache.old_header = NULL;
    bpool_destroy(&db->bpool);
    rwl_destroy(&db->global_rwl);
#if defined(IO_REQUIRES_SYNC)
    avmtx_destroy(&db->io_mtx);
#endif
    free(db);
}

static unsigned mask_to_power_of_two(unsigned x)
{
    unsigned cnt = 0;
    if (x == 0) return 0;
    while ((void)cnt++, x >>= 1)
        ;
    return 1 << (cnt - 1);
}

//static unsigned ulog2(unsigned x)
//{    
//    if (x > 1) {
//        unsigned cnt = 0;
//        while ((void)cnt++, x >>= 1)
//            ;
//        return (cnt - 1);
//    }
//    return 0;
//}

static int avstor_init(avstor **pdb, unsigned szcache)
{
    PageCache *cache;
    avstor *db;
    unsigned i;

    if (!(db = calloc(1, sizeof(*db)))) {
        return 0;
    }

    db->l2_size = szcache;
    cache = &db->cache;
    cache->l2_len = db->l2_size / (KB_PER_PAGE * L2_ASSOC);
    cache->l2_mask = cache->l2_len - 1;

    if (!rwl_init(&db->global_rwl)) {
        goto err_rwl_init;
    }
#if defined(IO_REQUIRES_SYNC)
    if (!avmtx_init(&db->io_mtx)) {
        goto err_avmtx_init;
    }
#endif

    if (!bpool_init(&db->bpool, 512 / DEFAULT_BLOCK_SIZE)) {
        goto err_bpool_init;
    }

    if (!(cache->header = avs_aligned_malloc(PAGE_SIZE * 2, PAGE_SIZE))) {
        avstor_destroy(db);
        return 0;
    }
    cache->old_header = PTR(cache->header, PAGE_SIZE);

    if (!(cache->rows = calloc(cache->l2_len, sizeof(CacheRow)))) {
        avstor_destroy(db);
        return 0;
    }

    for (i = 0; i < cache->l2_len; ++i) {
#if defined(AVSTOR_CONFIG_THREAD_SAFE)
        if (!rwl_init(&cache->rows[i].lock)) {
            avstor_destroy(db);
            return 0;
        }
#endif
        cache->rows[i].load_count = 1;
        cache->rows[i].capacity = L2_ASSOC;
        if (!(cache->rows[i].items = calloc(L2_ASSOC, sizeof(CacheItem)))) {
            avstor_destroy(db);
            return 0;
        }
    }
    *pdb = db;
    return 1;
err_bpool_init:
#if defined(IO_REQUIRES_SYNC)
    avmtx_destroy(&db->io_mtx);
err_avmtx_init:
#endif
    rwl_destroy(&db->global_rwl);
err_rwl_init:
    free(db);
    return 0;
}

static int read_page(avstor *db, avstor_off page_offset, AvPage *page)
{
    //PageCache *cache = &avs->cache;
    uint32_t checksum;
    int numread;
    numread = io_read(db, page, page_offset, PAGE_SIZE);
    if (0 == numread) {
        //THROW(AVSTOR_IOERR, "page offset beyond EOF.");
        return AVSTOR_IOERR;
    }
    else if (numread < 0) {
        //THROW(AVSTOR_IOERR, "fread failed.");
        return AVSTOR_IOERR;
    }
    else if (numread < PAGE_SIZE) {
        return AVSTOR_CORRUPT;
        //THROW(AVSTOR_CORRUPT, "fread read fewer than expected bytes.");
    }
    checksum = page->checksum;
    page->checksum = 0;
    if (checksum != compute_page_checksum(page)) {
        //THROW(AVSTOR_CORRUPT, "page checksum error.");
        page->checksum = checksum;
        return AVSTOR_CORRUPT;
    }
    page->checksum = checksum; // cache->l2_lru_count;
    return AVSTOR_OK;
}

static int write_page(avstor *db, AvPage* page)
{
    assert(atomic_load_int32_relaxed(&page->lock_count) == 0);
    if (is_page_dirty(page)) {
        int res;
        set_page_clean(page);
        update_page_checksum(page);
        res = io_write(db, page, page->page_offset, PAGE_SIZE);
        if (res < PAGE_SIZE) {
            set_page_dirty(page);
            return AVSTOR_IOERR;
        }
    }
    return AVSTOR_OK;
}

static __inline unsigned cache_get_row(PageCache *cache, avstor_off page_ofs)
{
    // multiplier from L'Ecuyer 1999
    return (unsigned)((((page_ofs / PAGE_SIZE) * 1597334677u) >> 3) & cache->l2_mask);
}

static __inline CacheItem* cache_lookup_scan_line(CacheRow *line, avstor_off page_ofs, CacheItem* *out_item)
{
    unsigned cnt;
    CacheItem *item, *avail_item = NULL;
    for (cnt = 0; cnt < line->capacity; ++cnt) {
        item = &line->items[cnt];
        if (!item->page) {
            *out_item = item;
            return NULL;
        }
        else if (item->offset == 0) {
            avail_item = item;
        }
        else if (item->offset == page_ofs) {
            return item;
        }
    }
    *out_item = avail_item;
    return NULL;
}

enum {
    evict_success = 0,
    evict_fail,
    evict_io_error,
    evict_must_flush
};

static int cache_evict(avstor *db, CacheRow *line, CacheItem* *out_item)
{
    CacheItem *poldest = NULL;
    unsigned col;
    uint32_t min_age = line->load_count;
    int auto_save = db->oflags & AVSTOR_OPEN_AUTOSAVE;

    /* Find oldest non-locked page */
    for (col = 0; col < line->capacity; ++col) {
        CacheItem* item = &line->items[col];
        AvPage *page = item->page;
        if (!page) {
            break;
        }
        else if (item->offset != 0 && item->load_time < min_age
                 && atomic_load_int32_relaxed(&page->lock_count) == 0) {
            min_age = item->load_time;
            poldest = item;
        }
    }

    if (poldest) {
        if (is_page_dirty(poldest->page)) {
            if (auto_save) {
                if (AVSTOR_OK != write_page(db, poldest->page)) {
                    return evict_io_error;
                }
            }
            else {
                return evict_must_flush;
            }
        }
        poldest->offset = 0;
        *out_item = poldest;
        return evict_success;
    }
    return evict_fail;
}

static CacheItem* cache_line_realloc(avstor *db, CacheRow *line)
{
    CacheItem* new_items;
    unsigned col;
    unsigned old_capacity = line->capacity;

    line->capacity += 4;
    if ((new_items = realloc(line->items, line->capacity * sizeof(CacheItem)))) {
        CacheItem* item;
        for (col = old_capacity; col < line->capacity; ++col) {
            new_items[col].load_time = 0;
            new_items[col].offset = 0;
            new_items[col].page = NULL;
        }
        line->items = new_items;
        item = &line->items[old_capacity];
        if ((item->page = bpool_alloc_page(&db->bpool))) {
            return item;
        }
    }
    return NULL;
}

static AvPage* cache_lookup(avstor *db, avstor_off page_ofs, int is_existing)
{
    PageCache *cache = &db->cache;
    CacheItem *item;
    AvPage *page;
    CacheRow *row;

    unsigned row_num = cache_get_row(cache, page_ofs);
    CacheItem *first_empty_item;
    int evict_result;
    assert(page_ofs != 0);
    row = &cache->rows[row_num];

    do {
        rwl_lock_shared(&row->lock);
        if ((item = cache_lookup_scan_line(row, page_ofs, &first_empty_item))) {
            // page was found in cache

            // This is OK because nobody else has exclusive lock on row, i.e. not trying to evict
            lock_page(item->page);

            rwl_release(&row->lock);
            return item->page;
        }
        // not in cache. Try to upgrade lock or retry lookup
    } while (!rwl_upgrade_or_release(&row->lock));

    // At this point the cache line is locked exclusively
    if (first_empty_item) {
        item = first_empty_item;
        if ((item->page = bpool_alloc_page(&db->bpool))) {
            goto skip_evict;
        }
        // Out of memory. We will have to evict.
    }

    evict_result = cache_evict(db, row, &item);

    if (evict_result != evict_success) {
        if (evict_result == evict_fail) {
            // This should almost never happen, maybe with exremely small cache sizes and many threads
            if (!(item = cache_line_realloc(db, row))) {
                rwl_release(&row->lock);
                THROW(AVSTOR_NOMEM, "cache_line_realloc failed: out of memory");
            }
        }
        else if (evict_result == evict_io_error) {
            rwl_release(&row->lock);
            THROW(AVSTOR_IOERR, "IO error during cache page flush");
        }
        else if (evict_result == evict_must_flush) {
            rwl_release(&row->lock);
            THROW(AVSTOR_ABORT, "Must flush but AUTOSAVE is off");
        }
    }

skip_evict:

    page = item->page;
    if (is_existing) {
        int result;
        // If looking for existing page, we can load it into the empty (or evicted) page        
        if (AVSTOR_OK != (result = read_page(db, page_ofs, page))) {
            rwl_release(&row->lock);
            THROW(result, "read_page() failed while reading page into cache");
        }
        item->load_time = row->load_count++;
    }
    else {
        // Clear the evicted or newly allocated page
        memset(page, 0, PAGE_SIZE);
        page->page_offset = page_ofs;
        item->load_time = 0;
    }
    item->offset = page_ofs;
    atomic_store_int32_release(&page->lock_count, 1);
    rwl_release(&row->lock);
    return page;
}

//static __inline void backtrace_init(AvStack* st, NodeRef* root)
//{
//    st->top = -1;
//    st->root = root;
//}

static __inline AvStackData* backtrace_push(AvStack* st)
{
    if (st->top < AVSTOR_AVL_HEIGHT - 1) {
        return &st->data[++st->top];
    }
    else {
        THROW(AVSTOR_INTERNAL, MSG_BACKTRACE_OVERFLOW);
    }
}

static __inline AvStackData* backtrace_pop(AvStack* st)
{
    return st->top >= 0 ? &st->data[st->top--] : NULL;
}

static __inline const AvStackData* backtrace_peek(const AvStack* st, int pos)
{
    return pos >= 0 ? &st->data[pos] : NULL;
}

static __inline AvStackData* backtrace_top(AvStack* st)
{
    return st->top >= 0 ? &st->data[st->top] : NULL;
}

static __inline void set_bf(AvNode *node, int bf)
{
    assert(bf >= -1 && bf <= 1);
    node->hdr = (node->hdr & ~BF_MASK) | ((uint16_t)(bf + 1));
}

static __inline AvPage* get_ptr_page(const void *ptr)
{
    return (AvPage*)((uintptr_t)ptr & PAGE_MASK);
}

#define set_ptr_dirty(node) set_page_dirty(get_ptr_page(node))

static __inline avstor_off get_ofs(const AvNode *node)
{
    return get_ptr_page(node)->page_offset + node->index;
}

static __inline NodeRef get_nref(const AvNode *node)
{
    return ofs_to_nref(get_ofs(node));
}

static void set_nref(const AvNode *src, NodeRef *dest)
{
    if (src) {
        *dest = get_nref(src);
    }
    else {
        *dest = NODEREF_NULL;
    }
    set_ptr_dirty(dest);
}

static __inline void assign_nref(NodeRef src, NodeRef *dest)
{
    *dest = src;
    set_ptr_dirty(dest);
}

static __inline AvNode* get_node(AvPage *page, unsigned index)
{
    //assert(*(uint16_t*)PTR(page, index) != INVALID_INDEX);
    uint16_t node_offset = *(uint16_t*)PTR(page, index);
    if (node_offset == INVALID_INDEX) {
        THROW(AVSTOR_INVOPER, "Node has been deleted.");
    }
    return PTR(page, node_offset);
}

#if !defined (NDEBUG)

static int is_node_addr_valid(const avstor *db, const AvNode* node)
{
    //return 1;
    AvPage* page = get_ptr_page(node);
    (void)db;
    if (!page) {
        goto error_node;
    }
    /* if (memcmp(&page->id, &PAGE_ID, sizeof(PAGE_ID)) != 0) {
         goto error_node;
     }*/
    if (get_node(page, node->index) != node) {
        goto error_node;
    }
    return 1;
error_node:
    errout ("Node pointer is invalid.");
    return 0;
}
#endif

static __inline void set_node_size(AvNode* node, unsigned nodesz)
{
    node->hdr = (node->hdr & ~NODE_SIZEMASK) | (uint16_t)(nodesz << 4u);
}

static unsigned get_page_free_space(AvPage* page)
{
    unsigned top = page->top;
    unsigned bottom = align_node(  //compensate for alignment
                                 offsetof(AvPage, index[page->index_count])
                                 + (page->index_freelist == INVALID_INDEX ? 2 : 0)); // compensate in case a new index has to be allocated      
    return (top > bottom) ? top - bottom : 0;
}

static __inline AvPage* get_page(avstor *db, avstor_off page_offset)
{
    return cache_lookup(db, page_offset, 1);
}

static AvNode* lock_node(avstor *db, const avstor_off noderef)
{
    assert(noderef != 0);
    return get_node(get_page(db, noderef & OFFSET_MASK), (unsigned)(noderef & ~OFFSET_MASK));
}

static AvNode* lock_node_ex(avstor *db, const NodeRef *noderef)
{
    AvPage *node_page;
    avstor_off pageofs, node_ofs;
    assert(noderef && !is_nref_empty(*noderef));
    node_page = get_ptr_page(noderef);
    assert(atomic_load_int32_relaxed(&node_page->lock_count) > 0);  // page containging noderef should already be locked
    node_ofs = nref_to_ofs(*noderef);
    pageofs = node_ofs & OFFSET_MASK;
    if (pageofs != node_page->page_offset) {
        /* This assumes page of noderef is already locked, otherwise it could get swapped out! */
        node_page = get_page(db, pageofs);
    }
    else {
        // This is ok because page is already locked, we're only increasing the lock count
        lock_page(node_page);
    }
    return get_node(node_page, (unsigned)(node_ofs & ~OFFSET_MASK));
}

static __inline void unlock_ptr(const void *ptr)
{
    assert(ptr);
    unlock_page(get_ptr_page(ptr));
}

static AvNode* lock_unlock_node(avstor *db, const avstor_off ofs, AvNode *node_to_unlock)
{
    AvPage *node_page;
    avstor_off pageofs;
    if (!node_to_unlock) {
        return lock_node(db, ofs);
    }
    node_page = get_ptr_page(node_to_unlock);
    assert(atomic_load_int32_relaxed(&node_page->lock_count) > 0);  // page containging noderef should already be locked
    pageofs = ofs & OFFSET_MASK;
    if (pageofs != node_page->page_offset) {
        unlock_ptr(node_to_unlock);
        node_page = get_page(db, pageofs);
    }
    else {
        // This is ok because page is already locked, we're only increasing the lock count
        //lock_page(node_page);
    }
    return get_node(node_page, (unsigned)(ofs & ~OFFSET_MASK));
}

static __inline void unlock_ptr_checked(const void *ptr)
{
    if (ptr) {
        unlock_ptr(ptr);
    }
}

static __inline void lock_ref(const NodeRef *noderef)
{
    AvPage *page = get_ptr_page(noderef);
    // Outside shared cache row lock, we can only increment lock count of currently locked page.
    // Otherwise, a page currently being evicted might end up getting re-locked, which would be bad.
    // Header is exception, it is never in the cache
    assert(atomic_load_int32_relaxed(&page->lock_count) > 0 || page->page_offset == 0);
    lock_page(page);
}

static AvNode* find_node_with_backtrace(avstor *db, const avstor_key *key, AvStack *st,
                                        NodeRef *root, NodeRef* volatile *out_ref)
{
    AvStackData *top = NULL;
    AvNode *cur = NULL;
    st->top = -1;
    st->root = root;
    if (out_ref) {
        *out_ref = NULL;
    }
    if (root && !is_nref_empty(*root)) {
        NodeRef *ref = root;
        int comp;
        lock_ref(ref);
        cur = lock_node_ex(db, ref);

        while (0 != (comp = key->comparer(key->buf, cur->name))) {
            top = backtrace_push(st);
            top->comp = comp;
            top->noderef = nref_to_ofs(*ref);
            unlock_ptr(ref);
            ref = (comp < 0) ? &cur->left : &cur->right;
            if (is_nref_empty(*ref)) {
                if (out_ref) {
                    *out_ref = ref;  // leave page of ref locked if returning it
                    return NULL;
                }
                else {
                    cur = NULL;
                    break;
                }
            }
            cur = lock_node_ex(db, ref);
        }
        unlock_ptr(ref);
    }
    return cur;
}

static void rotate_right(AvNode *x, AvNode *z)
{
    NodeRef t23 = z->right;
    assign_nref(t23, &x->left);
    set_nref(x, &z->right);
    if (BF(z) == 0) {
        set_bf(x, -1);
        set_bf(z, 1);
    }
    else {
        set_bf(x, 0);
        set_bf(z, 0);
    }
}

static void rotate_left(AvNode *x, AvNode *z)
{
    NodeRef t23 = z->left;
    assign_nref(t23, &x->right);
    set_nref(x, &z->left);
    if (BF(z) == 0) {
        set_bf(x, 1);
        set_bf(z, -1);
    }
    else {
        set_bf(x, 0);
        set_bf(z, 0);
    }
}

static AvNode* rotate_right_left(avstor *db, AvNode *x, AvNode *z)
{
    AvNode *y = lock_node_ex(db, &z->left);
    NodeRef t3 = y->right;
    NodeRef t2;
    assign_nref(t3, &z->left);
    set_nref(z, &y->right);
    t2 = y->left;
    assign_nref(t2, &x->right);
    set_nref(x, &y->left);
    if (BF(y) == 0) {
        set_bf(x, 0);
        set_bf(z, 0);
    }
    else if (BF(y) > 0) {
        set_bf(x, -1);
        set_bf(z, 0);
    }
    else {
        set_bf(x, 0);
        set_bf(z, 1);
    }
    set_bf(y, 0);
    unlock_ptr(z);
    return y;
}

static AvNode* rotate_left_right(avstor *db, AvNode *x, AvNode *z)
{
    AvNode *y = lock_node_ex(db, &z->right);
    NodeRef t3 = y->left;
    NodeRef t2;
    assign_nref(t3, &z->right);
    set_nref(z, &y->left);
    t2 = y->right;
    assign_nref(t2, &x->left);
    set_nref(x, &y->right);
    if (BF(y) == 0) {
        set_bf(x, 0);
        set_bf(z, 0);
    }
    else if (BF(y) < 0) {
        set_bf(x, 1);
        set_bf(z, 0);
    }
    else {
        set_bf(x, 0);
        set_bf(z, -1);
    }
    set_bf(y, 0);
    unlock_ptr(z);
    return y;
}

static void backtrace_set_ref(avstor *db, AvStack *st, int pos, AvNode *cur, AvNode *src)
{
    const AvStackData *data = backtrace_peek(st, pos);
    if (data) {
        avstor_off cur_ofs = get_ofs(cur);
        AvNode *dest = lock_node(db, data->noderef);
        NodeRef *dest_child;
        if (nref_to_ofs(dest->left) == cur_ofs) {
            dest_child = &dest->left;
        }
        else if (nref_to_ofs(dest->right) == cur_ofs) {
            dest_child = &dest->right;
        }
        else {
            unlock_ptr(dest);
            THROW(AVSTOR_INTERNAL, "dest is not a parent of cur");
        }
        set_nref(src, dest_child);
        unlock_ptr(dest);
    }
    else {
        set_nref(src, st->root);
    }
}

static void balance_down(avstor *db, AvStack *st)
{
    AvStackData *top;
    while ((top = backtrace_pop(st))) {
        AvNode *cur = lock_node(db, top->noderef);
        int comp = top->comp < 0 ? -1 : 1;  // because strcmp could return below -1 or above +1
        int bf_cur = BF(cur);
        if (bf_cur == 0) {
            // was balanced but either subtree increased in height
            set_bf(cur, comp);
            set_ptr_dirty(cur);
            unlock_ptr(cur);
        }
        else if ((comp + bf_cur) != 0) {
            //Was unbalanced and now even more unbalanced. Must rotate.
            AvNode *z;
            if (bf_cur > 0) {
                z = lock_node_ex(db, &cur->right);
                if (BF(z) > 0) {
                    rotate_left(cur, z);
                }
                else {
                    z = rotate_right_left(db, cur, z);
                }
            }
            else {
                z = lock_node_ex(db, &cur->left);
                if (BF(z) < 0) {
                    rotate_right(cur, z);
                }
                else {
                    z = rotate_left_right(db, cur, z);
                }
            }
            backtrace_set_ref(db, st, st->top, cur, z);
            unlock_ptr(z);
            unlock_ptr(cur);
            break;
        }
        else {
            // was unbalanced but now balanced
            set_bf(cur, 0);
            set_ptr_dirty(cur);
            unlock_ptr(cur);
            break;
        }
    }
}

static void balance_up(avstor *db, AvStack *st)
{
    AvStackData *top;
    while ((top = backtrace_pop(st))) {
        AvNode *cur = lock_node(db, top->noderef);
        int comp = top->comp;
        int bf_cur = BF(cur);
        int b;
        if (comp < 0) {
            if (bf_cur > 0) {
                AvNode *z = lock_node_ex(db, &cur->right);
                b = BF(z);
                if (b < 0) {
                    z = rotate_right_left(db, cur, z);
                }
                else {
                    rotate_left(cur, z);
                }
                backtrace_set_ref(db, st, st->top, cur, z);
                unlock_ptr(z);
                unlock_ptr(cur);
            }
            else {
                set_ptr_dirty(cur);
                if (bf_cur == 0) {
                    set_bf(cur, 1);
                    unlock_ptr(cur);
                    break;
                }
                set_bf(cur, 0);
                unlock_ptr(cur);
                continue;
            }
        }
        else {
            if (bf_cur < 0) {
                AvNode *z = lock_node_ex(db, &cur->left);
                b = BF(z);
                if (b > 0) {
                    z = rotate_left_right(db, cur, z);
                }
                else {
                    rotate_right(cur, z);
                }
                backtrace_set_ref(db, st, st->top, cur, z);
                unlock_ptr(z);
                unlock_ptr(cur);
            }
            else {
                set_ptr_dirty(cur);
                if (bf_cur == 0) {
                    set_bf(cur, -1);
                    unlock_ptr(cur);
                    break;
                }
                set_bf(cur, 0);
                unlock_ptr(cur);
                continue;
            }
        }
        if (b == 0) break;
    }
}

static void remove_node(avstor *db, AvNode *node, AvStack *st)
{
    AvStackData *top;
    NodeRef *ref;
    assert(node && st);

    if (!(top = backtrace_top(st))) {
        ref = st->root;
    }
    else {
        AvNode *temp = lock_node(db, top->noderef);
        ref = top->comp < 0 ? &temp->left : &temp->right;
        unlock_ptr(temp);
    }

    if (is_nref_empty(node->left) && is_nref_empty(node->right)) {
        // Case 1: node to remove has no children
        assign_nref(NODEREF_NULL, ref);
    }
    else if (is_nref_empty(node->left) || is_nref_empty(node->right)) {
        // Case 2: node has only one child
        NodeRef *childRef = (!is_nref_empty(node->left)) ? &node->left : &node->right;
        assign_nref(*childRef, ref);
        assign_nref(NODEREF_NULL, childRef);
    }
    else {
        // Case 3: node has two children
        // Find in-order successor (smallest in right subtree)
        AvNode *succ;
        AvStackData *topdel;
        AvNode* topdel_node;
        int delpos;
        top = backtrace_push(st);
        assert(top);
        top->noderef = get_ofs(node);
        top->comp = 1;
        ref = &node->right;
        lock_ref(ref);
        succ = lock_node_ex(db, ref);
        topdel = top;
        delpos = st->top;
        while (!is_nref_empty(succ->left)) {
            top = backtrace_push(st);
            assert(top);
            top->noderef = nref_to_ofs(*ref);
            top->comp = -1;
            unlock_ptr(ref);
            ref = &succ->left;
            //lock_ref(ref);
            //unlock_ptr(succ);            
            succ = lock_node_ex(db, ref);
        }
        assign_nref(node->left, &succ->left);
        if (topdel != top) {
            assign_nref(succ->right, ref);
            assign_nref(node->right, &succ->right);
        }
        unlock_ptr(ref);
        topdel_node = lock_node(db, topdel->noderef);
        backtrace_set_ref(db, st, delpos-1, topdel_node, succ);
        unlock_ptr(topdel_node);
        topdel->noderef = get_ofs(succ);
        set_bf(succ, BF(node));
        unlock_ptr(succ);
    }
    balance_up(db, st);
    assign_nref(NODEREF_NULL, &node->left);
    assign_nref(NODEREF_NULL, &node->right);
}

static AvPage* create_page(avstor *db, unsigned type)
{
    AvPage *hdr = db->cache.header;
    AvPage *page;
    avstor_off page_offset;

    if (hdr->pagecount == MAX_FILE_PAGES) {
        THROW(AVSTOR_INVOPER, "Maximum allowable file size exceeded");
    }
    page_offset = (avstor_off)hdr->pagecount * (unsigned)PAGE_SIZE;
    page = cache_lookup(db, page_offset, 0);
    //memcpy(&page->id, &PAGE_ID, sizeof(PAGE_ID));
    page->type = (uint8_t)type;
    page->top = PAGE_SIZE;
    page->index_freelist = INVALID_INDEX;
    set_page_dirty(page);
    hdr->pagecount++;
    set_page_dirty(hdr);

    return page;
}

static AvNode* alloc_node(avstor *db, AvPage *preferred_page, unsigned size, unsigned page_pool)
{
    AvPage *page = NULL;
    uint16_t *index;
    AvNode *node;
    unsigned index_ofs;
    uint16_t nextfree;

    if (preferred_page && size <= get_page_free_space(preferred_page)) {
        page = preferred_page;
        assert(atomic_load_int32_relaxed(&page->lock_count) > 0);
        lock_page(page);
        set_page_dirty(page);
    }
    else {
        uint32_t page_num = db->cache.header->page_pool[page_pool];
        if (page_num != 0) {
            page = get_page(db, (avstor_off)page_num * PAGE_SIZE);
            if (size > get_page_free_space(page)) {
                unlock_page(page);
                page = NULL;
            }
            else {
                set_page_dirty(page);
            }
        }
        if (!page || page_num == 0) {
            page = create_page(db, PAGE_KEYS);
            if (size > get_page_free_space(page)) {
                THROW(AVSTOR_INTERNAL, MSG_NO_SPACE_IN_PAGE);
            }
            db->cache.header->page_pool[page_pool] = (uint32_t)(page->page_offset / PAGE_SIZE);
        }
    }

    nextfree = page->index_freelist;
    //set_page_dirty(page);
    if (nextfree == INVALID_INDEX) {
        index = &page->index[page->index_count];
        page->index_count++;
    }
    else {
        index = (uint16_t*)PTR(page, nextfree);
        page->index_freelist = *index;
    }
    page->top -= size;
    *index = page->top;
    index_ofs = PTR_DIFF(page, index);
    node = get_node(page, index_ofs);

    // check if we have overwritten the node index array
    if ((void*)node < (void*)&page->index[page->index_count]) {
        THROW(AVSTOR_INTERNAL, MSG_PAGE_CORRUPTED);
    }

    node->index = (uint16_t)index_ofs;
    set_node_size(node, size);
    //lock_page(page);
    return node;
}

static AvNode* resize_node(AvNode* node, unsigned newsize)
{
    AvPage *page;
    AvNode *next, *dest, *cur, *src;
    unsigned oldsize, page_free, newtop, count;

    assert((newsize == 0 || newsize >= sizeof(AvNode)) && newsize == align_node(newsize));

    oldsize = get_node_size(node);
    if (newsize == oldsize) {
        return node;
    }
    page = get_ptr_page(node);
    page_free = get_page_free_space(page);
    if (newsize > oldsize && (newsize - oldsize) > page_free) {
        THROW(AVSTOR_INTERNAL, "resize_node() failed");
    }
    newtop = page->top + oldsize - newsize;
    next = PTR(node, oldsize);
    if (newsize == 0) { // free the node instead of resize
        //check if we deallocated the last index
        uint16_t* oldindex = (uint16_t*)PTR(page, node->index);
        if (node->index == (offsetof(AvPage, index) - 2 + page->index_count * sizeof(uint16_t))) {
            // yes, just decrease count and zero the last one
            *oldindex = 0;
            page->index_count--;
        }
        else {
            // no, add it to free index list
            *oldindex = page->index_freelist;
            page->index_freelist = node->index;
        }
    }
    src = PTR(page, page->top);
    dest = PTR(page, newtop);
    count = PTR_DIFF(src, node);
    set_node_size(node, newsize);
    if (newsize < oldsize) {
        memmove(dest, src, (size_t)count + newsize);
        memset(src, 0, (size_t)oldsize - newsize);
    }
    else {
        unsigned diff = newsize - oldsize;
        memmove(dest, src, (size_t)count + oldsize);
        memset(PTR(node, oldsize - diff), 0, diff);
    }
    // Adjust index offsets
    cur = dest;
    while (cur < next) {
        uint16_t* curslot = (uint16_t*)PTR(page, cur->index);
        *curslot = (uint16_t)((int)*curslot + ((int)oldsize - (int)newsize));
        assert(is_node_addr_valid(NULL, cur));
        cur = (AvNode*)PTR(cur, get_node_size(cur));
    }
    page->top = (uint16_t)newtop;
    return PTR(node, (int)oldsize - (int)newsize);
}

static void free_node(AvNode* node)
{
    resize_node(node, 0);
}

static void delete_node(avstor *db, AvNode *node, AvStack *st)
{
    remove_node(db, node, st);
    free_node(node);
}

static AvNode* create_node(avstor *db, AvPage *preferred_page, const avstor_key *key,
                           unsigned szvalue, unsigned type, unsigned level)
{
    AvNode *node;

    // Offset of the fixed portion
    // Size of header + length of name (including null termination), aligned
    unsigned data_ofs = align_node(SIZE_NODE_HDR + key->len);

    // Add size of fixed portion (if any) and size of variable portion (if any)
    // and align to get node size
    unsigned node_size = align_node(data_ofs + NODE_CLASS[type].szdata + szvalue);

    unsigned page_pool = (level > 127) ? 254 : (level << 1);
    if (type != AVSTOR_TYPE_KEY) {
        page_pool++;
    }
    node = alloc_node(db, preferred_page, node_size, page_pool);

    node->hdr = (node->hdr & ~NODE_TYPEMASK) | (uint16_t)((type) << 2);
    node->left = NODEREF_NULL;
    node->right = NODEREF_NULL;
    node->szname = (uint8_t)(data_ofs - SIZE_NODE_HDR);
    memcpy(&node->name, key->buf, key->len);

    return node;
}

/*
static void print_node(avstor* rb, AvNode* node, int level) {
    char buf[256];
    int i;
    for (i = 0; i < level; ++i) {
        buf[i] = ' ';
    }
    buf[i] = 0;
    if (node == NULL) {
        printf("%s<NULL>\n", buf);
        return;
    }
    printf("%s%s[%i]\n", buf, get_node_name_ptr(node), BF(node));
    print_node(rb, lock_node_ex(rb, &node->left), level + 2);
    print_node(rb, lock_node_ex(rb, &node->right), level + 2);
}

static int check_bf(avstor* rb, AvNode* node) {
    if (node != NULL) {
        int h1 = check_bf(rb, lock_node_ex(rb, &node->left));
        int h2 = check_bf(rb, lock_node_ex(rb, &node->right));
        int bf = h2 - h1;
        if (h1 > (h2 + 1) || h2 > (h1 + 1)) {
            fprintf(stderr, "...AVL property violated!\n");
        }
        if (BF(node) != bf) {
            fprintf(stderr, "...Balance Factor invalid for %s! %i vs actual %i\n", get_node_name_ptr(node), BF(node), bf);
        }
        return (h1 > h2 ? h1 : h2) + 1;
    }
    return 0;
}
*/

static void insert_node(avstor *db, AvNode *item, AvStack *st)
{
    AvStackData *top;
    if ((top = backtrace_top(st))) {
        AvNode *cur = lock_node(db, top->noderef);
        NodeRef *ref = top->comp < 0 ? &cur->left : &cur->right;
        assert(is_nref_empty(*ref));

        set_nref(item, ref);
        set_bf(item, 0);
        unlock_ptr(cur);
        // trace back on the stack of ancestors and rebalance
        balance_down(db, st);
    }
    else {
        set_nref(item, st->root);
        set_bf(item, 0);
    }
}

static AvNode* find_key(avstor *db, const avstor_key *key, const NodeRef *rootref)
{
    AvNode *cur;
    const NodeRef *ref = rootref;
    lock_ref(ref);
    while (!is_nref_empty(*ref)) {
        int comp;
        cur = lock_node_ex(db, ref);
        unlock_ptr(ref);
        comp = key->comparer(key->buf, cur->name);
        if (comp == 0) {
            return cur;
        }
        ref = (comp < 0) ? &cur->left : &cur->right;
    }
    unlock_ptr(ref);
    return NULL;
}

/* Note that errors here are not YET recoverable */
int AVCALL avstor_commit(avstor *db, int flush)
{
    PageCache *cache;
    unsigned row, col;
    int result;

    CHECK_PARAM(db);
    rwl_lock_exclusive(&db->global_rwl);
    TRY(ex)
    {
        cache = &db->cache;

        for (row = 0; row < cache->l2_len; ++row) {
            CacheRow *line = &cache->rows[row];
            for (col = 0; col < line->capacity; ++col) {
                AvPage *page = line->items[col].page;
                if (!page) {
                    break;
                }
                else {
                    if (AVSTOR_OK != (result = write_page(db, page))) {
                        THROW(result, "write_page() failed");
                    }
                }
            }
        }
        if (AVSTOR_OK != (result = write_page(db, cache->header))) {
            THROW(result, "write_page() failed while writing header");
        }
        if (flush && !io_commit(db->file)) {
            THROW(AVSTOR_IOERR, "commit() failed");
        }
        // save header for rollback purposes
        memcpy(cache->old_header, cache->header, PAGE_SIZE);
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&db->global_rwl);
    return result;
}

int AVCALL avs_check_cache_consistency(avstor *db)
{
    PageCache *cache = &db->cache;
    unsigned row, col;
    for (row = 0; row < cache->l2_len; ++row) {
        CacheRow *line = &cache->rows[row];
        for (col = 0; col < line->capacity; ++col) {
            AvPage *page = line->items[col].page;
            if (!page) {
                break;
            }
            else if (atomic_load_int32_relaxed(&page->lock_count) != 0)  {
                return AVSTOR_CORRUPT;
            }
        }
    }
    return AVSTOR_OK;
}

static AvNode* lock_noderef(const avstor_node *parent)
{
    AvNode *result = NULL;
    if (parent->ref == 0) {
        THROW(AVSTOR_PARAM, MSG_INVALID_PARAMETER);
    }
    result = lock_node(parent->db, parent->ref);
    assert(is_node_addr_valid(parent->db, result));
    return result;
}

static __inline AvNode* lock_keyref(const avstor_node *parent)
{
    AvNode* result = lock_noderef(parent);
    if (NODE_TYPE(result) != AVSTOR_TYPE_KEY) {
        unlock_ptr(result);
        THROW(AVSTOR_MISMATCH, MSG_TYPE_MISMATCH);
    }
    return result;
}

static __inline AvNode* lock_valueref(const avstor_node *parent, unsigned type)
{
    AvNode* result = lock_noderef(parent);
    if (NODE_TYPE(result) != type) {
        unlock_ptr(result);
        THROW(AVSTOR_MISMATCH, MSG_TYPE_MISMATCH);
    }
    return result;
}

static void rollback(avstor *db)
{
    unsigned row, col;
    PageCache *cache = &db->cache;
    (void)rwl_upgrade_or_lock_exclusive(&db->global_rwl);

    for (row = 0; row < cache->l2_len; ++row) {
        CacheRow *line = &cache->rows[row];
        for (col = 0; col < line->capacity; ++col) {
            AvPage *page = line->items[col].page;
            if (page && page->page_offset != 0) {
                if (is_page_dirty(page)) {
                    // invalidate modified cache item
                    //page->page_offset = 0;
                    line->items[col].offset = 0;
                }
                if (atomic_load_int32_relaxed(&page->lock_count) != 0) {
                    atomic_store_int32_release(&page->lock_count, 0);
                }
            }
        }
    }

    // restore unmodified header
    memcpy(cache->header, cache->old_header, PAGE_SIZE);
}

static __inline AvNodeData* get_node_data(AvNode *node)
{
    return (AvNodeData*)PTR(node, SIZE_NODE_HDR + (node)->szname);
}

static __inline void avstor_node_set(avstor_node *node, const avstor_off off, avstor *db)
{
    node->db = db;
    node->ref = off;
}

int AVCALL avstor_node_init(avstor *db, avstor_node *node)
{
    CHECK_PARAM(db && node);
    node->db = db;
    node->ref = 0;
    return AVSTOR_OK;
}

void AVCALL avstor_node_destroy(avstor_node *node)
{
    CHECK_PARAM(node);
    node->db = NULL;
    node->ref = 0;
}

int AVCALL avstor_create_key(const avstor_node *parent, const avstor_key *key, avstor_node *out_key)
{
    avstor *db;
    // volatile because modified in TRY and referenced in CATCH
    AvNode *volatile node = NULL, *volatile parent_node = NULL;
    NodeRef *volatile last_ref = NULL;
    int result;
    unsigned level;

    CHECK_PARAM(parent && parent->db && key);
    if (is_invalid_avstor_key(key)) {
        RETURN(AVSTOR_PARAM, MSG_INVALID_PARAMETER);
    }
    db = parent->db;
    rwl_lock_exclusive(&db->global_rwl);
    TRY(ex)
    {
        AvStack st;
        NodeRef *rootref;
        AvNodeData *ndata;
        if (parent->ref != 0) {
            AvNodeData *pdata;
            parent_node = lock_keyref(parent);
            pdata = get_node_data(parent_node);
            level = pdata->vkey.level + 1;
            rootref = &pdata->vkey.subkey_root;
        }
        else {
            level = 1; // level 0 is reserved
            rootref = &db->cache.header->root;
        }

        if ((node = find_node_with_backtrace(db, key, &st, rootref, &last_ref))) {
            if (out_key) {
                avstor_node_set(out_key, get_ofs(node), db);
            }
            THROW(AVSTOR_EXISTS, MSG_NODE_EXISTS);
        }
        node = create_node(db, get_ptr_page(last_ref), key, 0, AVSTOR_TYPE_KEY, level);
        ndata = get_node_data(node);
        ndata->vkey.value_root = NODEREF_NULL;
        ndata->vkey.subkey_root = NODEREF_NULL;
        ndata->vkey.level = (uint16_t)level;

        insert_node(db, node, &st);
        if (out_key) {
            avstor_node_set(out_key, get_ofs(node), db);
        }
        unlock_ptr(node);
        unlock_ptr_checked(last_ref);
        unlock_ptr_checked(parent_node);
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(last_ref);
        unlock_ptr_checked(node);
        unlock_ptr_checked(parent_node);
        rollback(db);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&db->global_rwl);
    return result;
}

static int create_var_value(const avstor_node *parent, const avstor_key *key,
                            const void *value, unsigned valuesz, unsigned type, avstor_node *out_value)
{
    avstor *db;
    AvNode *volatile node = NULL, *volatile parent_node = NULL;
    NodeRef *volatile last_ref = NULL;
    int result;

    CHECK_PARAM(parent && parent->db && key && value);
    if (is_invalid_avstor_key(key)) {
        RETURN(AVSTOR_PARAM, MSG_INVALID_PARAMETER);
    }
    db = parent->db;
    rwl_lock_exclusive(&db->global_rwl);
    TRY(ex)
    {
        AvStack st;
        AvNodeData *ndata;
        AvNode *fnode;
        AvNodeData *pdata;
        parent_node = lock_keyref(parent);
        pdata = get_node_data(parent_node);

        if ((fnode = find_node_with_backtrace(db, key, &st, &pdata->vkey.value_root, &last_ref))) {
            unlock_ptr(fnode);
            THROW(AVSTOR_EXISTS, MSG_NODE_EXISTS);
        }

        node = create_node(db, get_ptr_page(last_ref), key, valuesz, type, pdata->vkey.level);
        ndata = get_node_data(node);
        ndata->vvar.length = (uint8_t)valuesz;
        memcpy(PTR(ndata, NODE_CLASS[type].szdata), value, valuesz);
        insert_node(db, node, &st);
        if (out_value) {
            avstor_node_set(out_value, get_ofs(node), db);
        }
        unlock_ptr_checked(last_ref);
        unlock_ptr(node);
        unlock_ptr(parent_node);
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(last_ref);
        unlock_ptr_checked(node);
        unlock_ptr_checked(parent_node);
        rollback(db);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&db->global_rwl);
    return result;
}

int AVCALL avstor_create_string(const avstor_node *parent, const avstor_key *key,
                                const char *value, avstor_node *out_value)
{
    size_t len;
    CHECK_PARAM(value);
    len = strlen_l(value, MAX_STRING_LEN + 1) + 1;
    if (len == (MAX_STRING_LEN + 1)) {
        RETURN(AVSTOR_PARAM, MSG_INVALID_PARAMETER);
    }
    return create_var_value(parent, key, value, (unsigned)len, AVSTOR_TYPE_STRING, out_value);
}

int AVCALL avstor_create_binary(const avstor_node *parent, const avstor_key *key,
                                const void *value, size_t szvalue, avstor_node *out_value)
{
    if (szvalue > MAX_BINARY_LEN) {
        RETURN(AVSTOR_PARAM, MSG_INVALID_PARAMETER);
    }
    return create_var_value(parent, key, value, (unsigned)szvalue, AVSTOR_TYPE_BINARY, out_value);
}

int AVCALL avstor_create_int32(const avstor_node *parent, const avstor_key *key,
                               int32_t value, avstor_node *out_value)
{
    avstor *db;
    AvNode *volatile node = NULL, *volatile parent_node = NULL;
    NodeRef *volatile last_ref = NULL;
    int result;

    CHECK_PARAM(parent && parent->db && key);
    if (is_invalid_avstor_key(key)) {
        RETURN(AVSTOR_PARAM, MSG_INVALID_PARAMETER);
    }
    db = parent->db;
    rwl_lock_exclusive(&db->global_rwl);
    TRY(ex)
    {
        AvStack st;
        AvNode *fnode;
        AvNodeData *pdata;
        parent_node = lock_keyref(parent);
        pdata = get_node_data(parent_node);

        if ((fnode = find_node_with_backtrace(db, key, &st, &pdata->vkey.value_root , &last_ref))) {
            unlock_ptr(fnode);
            THROW(AVSTOR_EXISTS, MSG_NODE_EXISTS);
        }

        node = create_node(db, get_ptr_page(last_ref), key, 0, AVSTOR_TYPE_INT32, pdata->vkey.level);
        get_node_data(node)->v32.value = value;
        insert_node(db, node, &st);
        if (out_value) {
            avstor_node_set(out_value, get_ofs(node), db);
        }
        unlock_ptr_checked(last_ref);
        unlock_ptr(node);
        unlock_ptr(parent_node);
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(last_ref);
        unlock_ptr_checked(node);
        unlock_ptr_checked(parent_node);
        rollback(db);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&db->global_rwl);
    return result;
}

static int create_fixed64_value(const avstor_node *parent, const avstor_key *key,
                                int64_t value, unsigned type, avstor_node *out_value)
{
    avstor *db;
    AvNode *volatile node = NULL, *volatile parent_node = NULL;
    NodeRef *volatile last_ref = NULL;
    int result;

    CHECK_PARAM(parent && parent->db && key);
    if (is_invalid_avstor_key(key)) {
        RETURN(AVSTOR_PARAM, MSG_INVALID_PARAMETER);
    }
    db = parent->db;
    rwl_lock_exclusive(&db->global_rwl);
    TRY(ex)
    {
        AvStack st;
        AvNode *fnode;
        AvNodeData *pdata;
        parent_node = lock_keyref(parent);
        pdata = get_node_data(parent_node);
        if ((fnode = find_node_with_backtrace(db, key, &st, &pdata->vkey.value_root , &last_ref))) {        
            unlock_ptr(fnode);
            THROW(AVSTOR_EXISTS, MSG_NODE_EXISTS);
        }
        node = create_node(db, get_ptr_page(last_ref), key, 0, type, pdata->vkey.level);
        memcpy(&get_node_data(node)->v64.value, &value, sizeof(int64_t));
        insert_node(db, node, &st);
        if (out_value) {
            avstor_node_set(out_value, get_ofs(node), db);
        }
        unlock_ptr_checked(last_ref);
        unlock_ptr(node);
        unlock_ptr(parent_node);
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(last_ref);
        unlock_ptr_checked(node);
        unlock_ptr_checked(parent_node);
        rollback(db);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&db->global_rwl);
    return result;
}

int AVCALL avstor_create_int64(const avstor_node *parent, const avstor_key *key,
                               int64_t value, avstor_node *out_value)
{
    return create_fixed64_value(parent, key, value, AVSTOR_TYPE_INT64, out_value);
}

int AVCALL avstor_create_double(const avstor_node *parent, const avstor_key *key,
                                double value, avstor_node *out_value)
{
    AvValue64 v;
    v.as_dbl = value;
    return create_fixed64_value(parent, key, v.as_int64, AVSTOR_TYPE_DOUBLE, out_value);
}

static void create_backlink(avstor *db, AvStack *st, avstor_off link, avstor_off target)
{
    NodeRef *volatile last_ref = NULL;
    AvNode *volatile node = NULL, *volatile link_node = NULL;
    avstor_key link_key;
    link_key.buf = &target;
    link_key.len = sizeof(avstor_off);
    link_key.comparer = &offset_comparer;

    TRY(ex)
    {
        AvNodeData *ndata;
        if (!(node = find_node_with_backtrace(db, &link_key, st, &db->cache.header->root_links, &last_ref))) {
            node = create_node(db, get_ptr_page(last_ref), &link_key, 0, AVSTOR_TYPE_KEY, 0);
            ndata = get_node_data(node);
            ndata->vkey.level = 0;
            ndata->vkey.subkey_root = NODEREF_NULL;
            ndata->vkey.value_root = NODEREF_NULL;
            insert_node(db, node, st);
        }
        else {
            ndata = get_node_data(node);
        }
        unlock_ptr_checked(last_ref);
        last_ref = NULL;

        link_key.buf = &link;
        if ((link_node = find_node_with_backtrace(db, &link_key, st, &ndata->vkey.value_root, &last_ref))) {
            THROW(AVSTOR_INTERNAL, "Back link reference already exists");
        }
        link_node = create_node(db, get_ptr_page(last_ref), &link_key, 0, AVSTOR_TYPE_LINK, 0);
        get_node_data(link_node)->vLink.link = ofs_to_nref(link);
        insert_node(db, link_node, st);
    }
    FINALLY(ex)
    {
        unlock_ptr_checked(last_ref);
        unlock_ptr_checked(link_node);
        unlock_ptr_checked(node);
    }
    END_TRY(ex);
}

int AVCALL avstor_create_link(const avstor_node *parent, const avstor_key *key,
                              const avstor_node *target, avstor_node *out_value)
{
    avstor *db;
    AvNode *volatile node = NULL, *volatile parent_node = NULL;
    NodeRef *volatile last_ref = NULL;
    int result;

    CHECK_PARAM(parent && parent->db && key && target && (parent->db == target->db));
    if (is_invalid_avstor_key(key) || (target->ref == 0)) {
        RETURN(AVSTOR_PARAM, MSG_INVALID_PARAMETER);
    }
    db = parent->db;
    rwl_lock_exclusive(&db->global_rwl);
    TRY(ex)
    {
        AvStack st;
        AvNode *fnode;
        AvNodeData *pdata;
        avstor_off ofs;

        parent_node = lock_keyref(parent);
        pdata = get_node_data(parent_node);
        if ((fnode = find_node_with_backtrace(db, key, &st, &pdata->vkey.value_root , &last_ref))) {
            unlock_ptr(fnode);
            THROW(AVSTOR_EXISTS, MSG_NODE_EXISTS);
        }
        node = create_node(db, get_ptr_page(last_ref), key, 0, AVSTOR_TYPE_LINK, pdata->vkey.level);
        get_node_data(node)->vLink.link = ofs_to_nref(target->ref);
        insert_node(db, node, &st);
        ofs = get_ofs(node);
        unlock_ptr_checked(last_ref);
        unlock_ptr(node);
        last_ref = NULL; node = NULL;

        create_backlink(db, &st, ofs, target->ref);

        if (out_value) {
            avstor_node_set(out_value, ofs, db);
        }
        unlock_ptr(parent_node);
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(last_ref);
        unlock_ptr_checked(node);
        unlock_ptr_checked(parent_node);
        rollback(db);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&db->global_rwl);
    return result;
}

int AVCALL avstor_get_int32(const avstor_node *value, int32_t *out_val)
{
    AvNode *volatile node = NULL;
    int result;

    CHECK_PARAM(value && value->db && out_val);
    rwl_lock_shared(&value->db->global_rwl);
    TRY(ex)
    {
        node = lock_valueref(value, AVSTOR_TYPE_INT32);

        *out_val = get_node_data(node)->v32.value;
        unlock_ptr(node);
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(node);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&value->db->global_rwl);
    return result;
}

static int get_fixed64_value(const avstor_node *value, unsigned type, int64_t *out_val)
{
    AvNode *volatile node = NULL;
    int result;

    CHECK_PARAM(value && value->db && out_val);
    rwl_lock_shared(&value->db->global_rwl);
    TRY(ex)
    {
        node = lock_valueref(value, type);

        memcpy(out_val, &get_node_data(node)->v64.value, sizeof(int64_t));
        unlock_ptr(node);
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(node);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&value->db->global_rwl);
    return result;
}

int AVCALL avstor_get_int64(const avstor_node *value, int64_t *out_val)
{
    return get_fixed64_value(value, AVSTOR_TYPE_INT64, out_val);
}

int AVCALL avstor_get_double(const avstor_node *value, double *out_val)
{
    return get_fixed64_value(value, AVSTOR_TYPE_DOUBLE, &((AvValue64*)out_val)->as_int64);
}

static int get_var_value(const avstor_node* value, void* buf,
                         size_t szbuf, unsigned type, size_t *out_bytes, uint32_t *out_length)
{
    AvNode *volatile node = NULL;
    int result;

    CHECK_PARAM(value && value->db && buf && out_bytes && out_length);
    rwl_lock_shared(&value->db->global_rwl);
    TRY(ex)
    {
        const AvNodeData *ndata;
        size_t bytes_copied;
        node = lock_valueref(value, type);
        ndata = get_node_data(node);
        bytes_copied = ndata->vvar.length > szbuf ? szbuf : (size_t)ndata->vvar.length;
        *out_length = ndata->vvar.length;
        *out_bytes = bytes_copied;
        memcpy(buf, CONST_PTR(ndata, NODE_CLASS[type].szdata), bytes_copied);
        unlock_ptr(node);
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(node);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&value->db->global_rwl);
    return result;
}

int AVCALL avstor_get_string(const avstor_node* value, char* buf,
                             size_t szbuf, uint32_t *out_length)
{
    size_t out_bytes = 0;
    int res = get_var_value(value, buf, szbuf, AVSTOR_TYPE_STRING, &out_bytes, out_length);
    if (res != AVSTOR_OK) {
        return res;
    }
    if (szbuf > 0) {
        if (out_bytes == szbuf) {
            out_bytes = szbuf - 1;
        }
        buf[out_bytes] = 0;
    }
    (*out_length)--;
    return AVSTOR_OK;
}

int AVCALL avstor_get_binary(const avstor_node* value, void* buf,
                             size_t szbuf, size_t *out_bytes, uint32_t *out_length)
{
    return get_var_value(value, buf, szbuf, AVSTOR_TYPE_BINARY, out_bytes, out_length);
}

int AVCALL avstor_get_link(const avstor_node *value, avstor_node *out_target)
{
    avstor *db;
    AvNode *volatile node = NULL;
    int result;

    CHECK_PARAM(value && value->db && out_target);
    db = value->db;
    rwl_lock_shared(&db->global_rwl);
    TRY(ex)
    {
        node = lock_valueref(value, AVSTOR_TYPE_LINK);
        avstor_node_set(out_target, nref_to_ofs(get_node_data(node)->vLink.link), db);
        unlock_ptr(node);
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(node);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&db->global_rwl);
    return result;
}

int AVCALL avstor_get_value(const avstor_node* value, void* buf,
                            size_t szbuf, unsigned *out_type, size_t *out_bytes, uint32_t *out_length)
{
    AvNode *volatile node = NULL;
    int result;

    CHECK_PARAM(value && value->db && buf && out_bytes && out_length);
    rwl_lock_shared(&value->db->global_rwl);
    TRY(ex)
    {
        unsigned node_type, szdata, data_offset;
        const AvNodeClass *node_class;
        const AvNodeData *ndata;
        size_t bytes_copied = 0;
        node = lock_noderef(value);
        node_type = NODE_TYPE(node);
        if (node_type == AVSTOR_TYPE_KEY) {
            THROW(AVSTOR_MISMATCH, MSG_TYPE_MISMATCH);
        }
        ndata = get_node_data(node);
        node_class = &NODE_CLASS[node_type];
        szdata = node_class->szdata;
        if (node_class->flags & NODE_FLAG_VAR) {
            // Node with variable sized data
            data_offset = szdata;
            bytes_copied = ndata->vvar.length > szbuf ? szbuf : (size_t)ndata->vvar.length;
            *out_length = ndata->vvar.length;
        }
        else {
            // Node with fixed size data only
            data_offset = 0;
            bytes_copied = szdata <= szbuf ? szdata : szbuf;
            *out_length = szdata;
        }
        memcpy(buf, CONST_PTR(ndata, data_offset), bytes_copied);
        *out_bytes = bytes_copied;
        *out_type = node_type;
        unlock_ptr(node);
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(node);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&value->db->global_rwl);
    return result;
}

int AVCALL avstor_update_int32(const avstor_node *value, int32_t new_val)
{
    AvNode *volatile node = NULL;
    int result;

    CHECK_PARAM(value && value->db);
    rwl_lock_exclusive(&value->db->global_rwl);
    TRY(ex)
    {
        node = lock_valueref(value, AVSTOR_TYPE_INT32);
        get_node_data(node)->v32.value = new_val;
        set_ptr_dirty(node);
        unlock_ptr(node);
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(node);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&value->db->global_rwl);
    return result;
}

static int update_fixed64_value(const avstor_node *value, unsigned type, int64_t new_val)
{
    AvNode *volatile node = NULL;
    int result;

    CHECK_PARAM(value && value->db);
    rwl_lock_exclusive(&value->db->global_rwl);
    TRY(ex)
    {
        node = lock_valueref(value, type);
        memcpy(&get_node_data(node)->v64.value, &new_val, sizeof(int64_t));
        set_ptr_dirty(node);
        unlock_ptr(node);
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(node);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&value->db->global_rwl);
    return result;
}

int AVCALL avstor_update_int64(const avstor_node *value, int64_t new_val)
{
    return update_fixed64_value(value, AVSTOR_TYPE_INT64, new_val);
}

int AVCALL avstor_update_double(const avstor_node *value, double new_val)
{
    AvValue64 v;
    v.as_dbl = new_val;
    return update_fixed64_value(value, AVSTOR_TYPE_DOUBLE, v.as_int64);
}

static int update_var_value(const avstor_node* value, const void* buf,
                            unsigned szbuf, unsigned type)
{
    AvNode *volatile node = NULL;
    int result;

    CHECK_PARAM(value && value->db && buf);
    rwl_lock_exclusive(&value->db->global_rwl);
    TRY(ex)
    {
        AvNodeData *ndata;
        unsigned szdata = NODE_CLASS[type].szdata;
        node = lock_valueref(value, type);
        ndata = get_node_data(node);
        if (szbuf != ndata->vvar.length) {
            node = resize_node(node, align_node(SIZE_NODE_HDR + node->szname + szdata + szbuf));
            ndata = get_node_data(node);
            ndata->vvar.length = (uint8_t)szbuf;
        }
        memcpy(PTR(ndata, szdata), buf, szbuf);
        set_ptr_dirty(node);
        unlock_ptr(node);
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(node);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&value->db->global_rwl);
    return result;
}


int AVCALL avstor_update_string(const avstor_node* value, const char* new_value)
{
    size_t len;

    CHECK_PARAM(new_value);
    len = strlen_l(new_value, MAX_STRING_LEN + 1) + 1;
    if (len == (MAX_STRING_LEN + 1)) {
        RETURN(AVSTOR_PARAM, MSG_INVALID_PARAMETER);
    }
    return update_var_value(value, new_value, (unsigned)len, AVSTOR_TYPE_STRING);
}

int AVCALL avstor_update_binary(const avstor_node* value, const void* new_value, size_t szvalue)
{
    if (szvalue > MAX_BINARY_LEN) {
        RETURN(AVSTOR_PARAM, MSG_INVALID_PARAMETER);
    }
    return update_var_value(value, new_value, (unsigned)szvalue, AVSTOR_TYPE_BINARY);
}

static void AVCALL db_open_file(avstor *db, const char* filename, int oflags)
{
    AvPage hdr;
    int bytes_read, result;

    result = io_open(filename, oflags);
    if (result == AVSTOR_INVALID_HANDLE) {
        THROW(AVSTOR_IOERR, "Failed to open file");
    }
    db->file = result;
    bytes_read = io_read(db, &hdr, 0, (unsigned)SIZE_PAGE_HDR);
    if (bytes_read < 0) {
        THROW(AVSTOR_IOERR, "Failed to read header.");
    }
    if (bytes_read < (int)SIZE_PAGE_HDR) {
        THROW(AVSTOR_CORRUPT, "Invalid header.");
    }
    /*if (memcmp(&hdr.id, &FILE_ID, sizeof(FILE_ID)) != 0) {
        THROW(AVSTOR_CORRUPT, "page id is not 'AVST'.");
    }*/

    if (hdr.pagesize != PAGE_SIZE) {
        THROW(AVSTOR_CORRUPT, "Invalid page size.");
    }
    if (AVSTOR_OK != (result = read_page(db, 0, db->cache.header))) {
        THROW(result, "read_page() failed while reading header.");
    }
    memcpy(db->cache.old_header, db->cache.header, PAGE_SIZE);
}

static void AVCALL db_create_file(avstor *db, const char* filename, int oflags)
{
    AvPage *hdr = NULL;
    int result;

    result = io_create(filename, oflags);
    if (result == AVSTOR_INVALID_HANDLE) {
        THROW(AVSTOR_IOERR, "Failed to create file");
    }
    db->file = result;

    hdr = db->cache.header;
    memset(hdr, 0, PAGE_SIZE);
    //memcpy(&hdr->id, &FILE_ID, sizeof(FILE_ID));
    hdr->page_offset = 0;
    hdr->type = PAGE_HDR;
    set_page_dirty(hdr);
    hdr->pagecount = 1;
    hdr->pagesize = PAGE_SIZE;
    hdr->root = NODEREF_NULL;
#if defined(AVSTOR_CONFIG_FILE_64BIT)
    hdr->flags = AVSTOR_FILE_64BIT;
#endif
    if (AVSTOR_OK != (result = avstor_commit(db, 1))) {
        THROW(result, "Failed to initialize file");
    }
}

int AVCALL avstor_open(avstor **pdb, const char* filename, unsigned szcache, int oflags)
{
    avstor *db;
    int result;

    CHECK_PARAM(pdb && filename);
    if (((oflags & AVSTOR_OPEN_CREATE) && (oflags & AVSTOR_OPEN_READONLY))
        || (!(oflags & AVSTOR_OPEN_READWRITE) && !(oflags & AVSTOR_OPEN_READONLY))) {
        RETURN(AVSTOR_PARAM, MSG_INVALID_FLAGS_COMBINATION);
    }

    szcache = mask_to_power_of_two(szcache);
    if (szcache < 64) {
        RETURN(AVSTOR_PARAM, MSG_INVALID_ATTRIBUTE);
    }
#if defined(DOS16)
    /* 16-bit DOS limited to 512K of cache since it has to be in conventional memory */
    if (szcache > 512) {
        szcache = 512;
    }
#endif
    if (!avstor_init(&db, szcache)) {
        RETURN(AVSTOR_NOMEM, MSG_OUT_OF_MEMORY);
    }

    db->oflags = oflags;

    TRY(ex)
    {
        if (oflags & AVSTOR_OPEN_CREATE) {
            db_create_file(db, filename, oflags);
        }
        else {
            db_open_file(db, filename, oflags);
        }
        *pdb = db;
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        avstor_destroy(db);
        result = ex.err;
    }
    END_TRY(ex);
    return result;
}

int AVCALL avstor_close(avstor *db)
{
    CHECK_PARAM(db);
    if (db->file != AVSTOR_INVALID_HANDLE) {
        io_close(db->file);
        db->file = AVSTOR_INVALID_HANDLE;
    }

    avstor_destroy(db);
    return AVSTOR_OK;
}

int AVCALL avstor_find(const avstor_node *parent, const avstor_key *key,
                       int flags, avstor_node *out_key)
{
    avstor *db;
    AvNode *volatile parent_node = NULL;
    AvNode *out_node = NULL;
    int result;
    int isvalue = (flags & AVSTOR_VALUES);

    CHECK_PARAM(parent && parent->db && key && out_key);
    if (is_invalid_avstor_key(key) || (isvalue && parent->ref == 0)) {
        RETURN(AVSTOR_PARAM, MSG_INVALID_PARAMETER);
    }
    db = parent->db;
    rwl_lock_shared(&db->global_rwl);
    TRY(ex)
    {
        NodeRef *ref;
        if (parent->ref != 0) {
            parent_node = lock_keyref(parent);
        }

        if (isvalue) {
            ref = &get_node_data(parent_node)->vkey.value_root;
        }
        else {
            ref = !parent_node ? &db->cache.header->root : &get_node_data(parent_node)->vkey.subkey_root;
        }

        if ((out_node = find_key(db, key, ref))) {
            if (out_key) {
                avstor_node_set(out_key, get_ofs(out_node), db);
            }
            unlock_ptr(out_node);
            result = AVSTOR_OK;
        }
        else {
            result = AVSTOR_NOTFOUND;
        }
        unlock_ptr_checked(parent_node);
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(parent_node);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&db->global_rwl);
    return result;
}

int AVCALL avstor_get_name(const avstor_node *value, avstor_key *key)
{
    AvNode *volatile node = NULL;
    int result;
#if defined(AVSTOR_CONFIG_THREAD_SAFE)
    avstor *db;
    CHECK_PARAM(value && value->db && key);
    db = value->db;
#else 
    CHECK_PARAM(value && value->db && key);
#endif

    rwl_lock_shared(&db->global_rwl);
    TRY(ex)
    {
        size_t szname;
        node = lock_noderef(value);
        szname = node->szname;
        if (szname > key->len) {
            THROW(AVSTOR_PARAM, MSG_INVALID_PARAMETER);
        }
        memcpy(key->buf, node->name, szname);
        unlock_ptr(node);
        result = AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(node);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&db->global_rwl);
    return result;
}

int AVCALL avstor_get_type(const avstor_node* value, unsigned *out_type)
{
    AvNode *volatile node = NULL;
    int result;
#if defined(AVSTOR_CONFIG_THREAD_SAFE)
    avstor *db;
    CHECK_PARAM(value && value->db && out_type);
    db = value->db;
#else 
    CHECK_PARAM(value && value->db && out_type);
#endif

    rwl_lock_shared(&db->global_rwl);
    TRY(ex)
    {
        node = lock_noderef(value);

        *out_type = (unsigned)NODE_TYPE(node);
        unlock_ptr(node);
        return AVSTOR_OK;
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(node);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&db->global_rwl);
    return result;
}

static int exists_link_to_node(avstor *db, AvNode *target)
{
    AvNode *link_node;
    avstor_off link_ofs = get_ofs(target);
    int result;
    avstor_key link_key;
    link_key.buf = &link_ofs;
    link_key.len = sizeof(link_ofs);
    link_key.comparer = offset_comparer;

    link_node = find_key(db, &link_key, &db->cache.header->root_links);
    result = link_node != NULL;
    unlock_ptr_checked(link_node);
    return result;
}

static void delete_backlink(avstor *db, AvNode *node)
{
    AvStack st;
    AvNode *volatile link_node = NULL, *volatile link_value = NULL;
    avstor_off link_ofs = nref_to_ofs(get_node_data(node)->vLink.link);
    avstor_key link_key;
    link_key.buf = &link_ofs;
    link_key.len = sizeof(link_ofs);
    link_key.comparer = offset_comparer;

    TRY(ex)
    {
        if ((link_node = find_node_with_backtrace(db, &link_key, &st, &db->cache.header->root_links, NULL))) {
            AvStack st_link;
            AvNodeData *lk_data = get_node_data(link_node);

            link_ofs = get_ofs(node);
            if ((link_value = find_node_with_backtrace(db, &link_key, &st_link, &lk_data->vkey.value_root, NULL))) {
                delete_node(db, link_value, &st_link);
                unlock_ptr(link_value);
                link_value = NULL;
            }
            if (is_nref_empty(lk_data->vkey.value_root)) {
                // If we have deleted the last value, delete the parent key as well
                delete_node(db, link_node, &st);
                unlock_ptr(link_node);
                link_node = NULL;
            }
        }
    }
    FINALLY(ex)
    {
        unlock_ptr_checked(link_value);
        unlock_ptr_checked(link_node);
    }
    END_TRY(ex);
}

int AVCALL avstor_delete(const avstor_node *parent, int flags, const avstor_key *key)
{
    AvStack st;
    avstor *db;
    AvNode *volatile node = NULL, *volatile parent_node = NULL;
    NodeRef *volatile last_ref = NULL;
    NodeRef *rootref;
    int result;
    int isvalue = flags & AVSTOR_VALUES;

    CHECK_PARAM(parent && parent->db && key && key);
    if (is_invalid_avstor_key(key) || (isvalue && parent->ref == 0)) {
        RETURN(AVSTOR_PARAM, MSG_INVALID_PARAMETER);
    }
    db = parent->db;
    TRY(ex)
    {
        while (1) {
            rwl_lock_shared(&db->global_rwl);
            if (parent->ref != 0) {
                parent_node = lock_keyref(parent);
            }

            if (isvalue) {
                rootref = &get_node_data(parent_node)->vkey.value_root;
            }
            else {
                rootref = !parent_node ? &db->cache.header->root : &get_node_data(parent_node)->vkey.subkey_root;
            }
            if ((node = find_node_with_backtrace(db, key, &st, rootref, &last_ref))) {
                if (NODE_TYPE(node) == AVSTOR_TYPE_KEY) {
                    AvNodeData *ndata = get_node_data(node);
                    if (!is_nref_empty(ndata->vkey.subkey_root) || !is_nref_empty(ndata->vkey.value_root)) {
                        THROW(AVSTOR_INVOPER, "Node has subkeys and/or values, unable to delete");
                    }
                }
                if (exists_link_to_node(db, node)) {
                    THROW(AVSTOR_INVOPER, "Node is a target of a link reference, unable to delete");
                }
#ifdef AVSTOR_CONFIG_THREAD_SAFE
                if (!rwl_upgrade(&db->global_rwl)) {
                    unlock_ptr_checked(last_ref);
                    unlock_ptr_checked(node);
                    unlock_ptr_checked(parent_node);
                    parent_node = NULL;
                    node = NULL;
                    last_ref = NULL;
                    rwl_release(&db->global_rwl);
                    continue;
                }
#endif
                if (NODE_TYPE(node) == AVSTOR_TYPE_LINK) {
                    /* if deleting link, we must also delete backlink */
                    delete_backlink(db, node);
                }
                delete_node(db, node, &st);
                unlock_ptr(node);
                result = AVSTOR_OK;
            }
            else {
                unlock_ptr_checked(last_ref);
                result = AVSTOR_NOTFOUND;
            }
            break;
        }
        unlock_ptr_checked(parent_node);
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(last_ref);
        unlock_ptr_checked(node);
        unlock_ptr_checked(parent_node);
        rollback(db);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&db->global_rwl);
    return result;
}

//static __inline void inorder_state_init(avstor_inorder *st, int flags)
//{
//    st->top = -1;
//    st->flags = flags;
//}

static __inline int inorder_state_isempty(avstor_inorder *st)
{
    return st->top < 0;
}

static __inline int inorder_state_push(avstor_inorder *st, avstor_off item)
{
    if (st->top < AVSTOR_AVL_HEIGHT - 1) {
        st->ref[++st->top] = item;
        return 1;
    }
    else {
        return 0;
    }
}

static __inline avstor_off inorder_state_pop(avstor_inorder *st)
{
    assert(st->top >= 0);
    return st->ref[st->top--];
}

static __inline avstor_off inorder_state_top(avstor_inorder *st)
{
    assert(st->top >= 0);
    return st->ref[st->top];
}

static avstor_off find_node_for_inorder(avstor_inorder *st, const avstor_key *key, avstor_off ofs)
{
    AvNode *cur = NULL;
    avstor_off result = 0;
    int is_descending = (st->flags & AVSTOR_DESCENDING);
    avstor *db = st->db;

    if (ofs != 0) {
        int comp;
        cur = lock_node(db, ofs);
        comp = key->comparer(key->buf, cur->name);

        while (1) {
            if (((is_descending ? -comp : comp) <= 0) && !inorder_state_push(st, ofs)) {
                // Push node if greater than or equal to name
                unlock_ptr(cur);
                THROW(AVSTOR_CORRUPT, MSG_BACKTRACE_OVERFLOW);
            }
            if (comp == 0) {
                result = ofs;
                break;  // Node found
            }
            ofs = nref_to_ofs((comp < 0) ? cur->left : cur->right);
            if (ofs == 0) {
                break;  // Node not found
            }
            cur = lock_unlock_node(db, ofs, cur);
            comp = key->comparer(key->buf, cur->name);
        }
        unlock_ptr(cur);
    }
    return result;
}

static int inorder_next(avstor_inorder *st, avstor_off ofs, avstor_node *out_node)
{
    AvNode *node = NULL;
    int is_descending = (st->flags & AVSTOR_DESCENDING);
    while (st->top >= 0 || ofs != 0) {
        if (ofs != 0) {
            if (!inorder_state_push(st, ofs)) {
                unlock_ptr_checked(node);
                THROW(AVSTOR_CORRUPT, MSG_BACKTRACE_OVERFLOW);
            }
            node = lock_unlock_node(st->db, ofs, node);
            ofs = nref_to_ofs(is_descending ? node->right : node->left);
        }
        else {
            if (inorder_state_isempty(st)) {
                unlock_ptr_checked(node);
                THROW(AVSTOR_CORRUPT, MSG_BACKTRACE_UNDERFLOW);
            }
            unlock_ptr_checked(node);
            out_node->ref = inorder_state_top(st);
            return AVSTOR_OK;
        }
    }
    unlock_ptr_checked(node);
    st->top = -1;
    return AVSTOR_NOTFOUND;
}

int AVCALL avstor_inorder_first(avstor_inorder *st, const avstor_node *parent, const avstor_key *key,
                                int flags, avstor_node *out_node)
{
    avstor *db;
    AvNode *volatile parent_node = NULL;
    int result;
    int isvalue = (flags & AVSTOR_VALUES);

    CHECK_PARAM(st && parent && parent->db && out_node);
    if ((key && is_invalid_avstor_key(key)) || (isvalue && parent->ref == 0)) {
        RETURN(AVSTOR_PARAM, MSG_INVALID_PARAMETER);
    }
    db = parent->db;

    st->db = db;
    st->top = -1;
    st->flags = flags;
    rwl_lock_shared(&db->global_rwl);
    TRY(ex)
    {
        avstor_off ofs;
        if (parent->ref != 0) {
            parent_node = lock_keyref(parent);
        }
        if (isvalue) {
            ofs = nref_to_ofs(get_node_data(parent_node)->vkey.value_root);
        }
        else {
            ofs = nref_to_ofs(!parent_node ? db->cache.header->root : get_node_data(parent_node)->vkey.subkey_root);
        }
        unlock_ptr_checked(parent_node);
        parent_node = NULL;
        if (key) {
            avstor_off fref = find_node_for_inorder(st, key, ofs);
            if (fref != 0) {
                avstor_node_set(out_node, fref, db);
                result = AVSTOR_OK;
            }
            else {
                // if node not found, next highest (or lowest) one is on the top of the stack
                if (!inorder_state_isempty(st)) {
                    //don't pop yet, right (or left) subtree needs to be traversed.
                    avstor_node_set(out_node, inorder_state_top(st), db);
                    result = AVSTOR_OK;
                }
                else {
                    result = AVSTOR_NOTFOUND;
                }
            }
        }
        else {
            result = inorder_next(st, ofs, out_node);
        }
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(parent_node);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&db->global_rwl);
    return result;
}

int AVCALL avstor_inorder_next(avstor_inorder *st, avstor_node *out_node)
{
    AvNode *volatile node = NULL;
    int result;

    CHECK_PARAM(st && out_node);
    if (inorder_state_isempty(st)) {
        avstor_node_set(out_node, 0, st->db);
        return AVSTOR_NOTFOUND;
    }
    rwl_lock_shared(&st->db->global_rwl);
    TRY(ex)
    {
        avstor_off ofs;
        node = lock_node(st->db, inorder_state_pop(st));
        ofs = nref_to_ofs((st->flags & AVSTOR_DESCENDING) ? node->left : node->right);
        unlock_ptr(node);
        node = NULL;
        result = inorder_next(st, ofs, out_node);
    }
    CATCH_ANY(ex)
    {
        unlock_ptr_checked(node);
        result = ex.err;
    }
    END_TRY(ex);
    rwl_release(&st->db->global_rwl);
    return result;
}

const char* AVCALL avstor_get_errstr(void)
{
    return last_err_msg;
}

#if defined(_WINDLL)
static void init_tls_vars(void)
{
    cur_ex = NULL;
    last_err_msg = NULL;
}

static void tls_dealloc(void)
{
    AvTLSData *data = (AvTLSData*)TlsGetValue(tls_idx);
    if (data) {
        free(data);
        TlsSetValue(tls_idx, NULL);
    }
}

BOOL WINAPI DllMain(
    HINSTANCE hinstDLL,
    DWORD fdwReason,
    LPVOID lpvReserved)
{
    AvTLSData *data;
    (void)hinstDLL;

    switch (fdwReason) {
    case DLL_PROCESS_ATTACH:
        tls_idx = TlsAlloc();
        if (tls_idx == TLS_OUT_OF_INDEXES) {
            return FALSE;
        }
        FALLTHROUGH;
    case DLL_THREAD_ATTACH:
        data = malloc(sizeof(*data));
        if (!data || !TlsSetValue(tls_idx, data)) {
            return FALSE;
        }
        init_tls_vars();
        break;
    case DLL_THREAD_DETACH:
        tls_dealloc();
        break;
    case DLL_PROCESS_DETACH:
        if (lpvReserved) {
            break;
        }
        tls_dealloc();
        TlsFree(tls_idx);
        break;
    default:
        return FALSE;
    }
    return TRUE;
}

#endif //_WINDLL
