
/*
* Basic C11-style threading library
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

#ifndef THREADS_H
#define THREADS_H

#include <time.h>

#if defined(__OS2__)

#undef _WIN32
#if !defined(OS2_INCLUDED)

#define INCL_DOS
#define INCL_DOSERRORS
#define INCL_DOSPROCESS
#include <os2.h>

#endif
#elif defined(_WIN32)

#include <Windows.h>
#endif

#include "stdatomic.h"

enum {
    thrd_success,
    thrd_nomem,
    thrd_timedout,
    thrd_busy,
    thrd_error
};

enum {
    mtx_plain = 0,
    mtx_recursive = 1 << 0,
    mtx_timed = 1 << 1
};

#define ONCE_FLAG_INIT { 0 }

typedef atomic_int once_flag;

typedef void (*tss_dtor_t)(void*);

typedef struct {
    unsigned int _key;
} tss_t;

typedef struct _usem {
#if defined(_WIN32)
    CRITICAL_SECTION    _cs;
    HANDLE              _event;
#elif defined(__OS2__)
    ULONG   _event_sem;
    ULONG   _mtx_sem;
#endif
    int _max_count;
    int _sema_count;
	int _waiters;
} _usem;

typedef struct mtx {    
    volatile atomic_int _lock;
    volatile atomic_int _count;
    int                 _type;
    struct _usem        _wait_sem;
} mtx_t;

#if defined(_WIN32)

typedef struct {
    void*       _Handle;
    unsigned    _ThreadID;
} thrd_t;

typedef struct {
    volatile atomic_int _state;
    void                *_ksem;
} cnd_t;

#elif defined(__OS2__)

struct _tld;

typedef struct {
    struct _tld         *_thr_data;
    int                 _thr_id;
} thrd_t;

struct _wait_item {
    ULONG               _wait_sema;
    struct _wait_item   *_pred;
    struct _wait_item   *_next;
};

typedef struct {
    struct _wait_item   *_head;
    struct _wait_item   *_tail;
    int                 _sema_count;
    int                 _max_waiters;
    mtx_t               _mtx;
} cnd_t;

extern void* __ThreadStackAddr;

#endif

typedef int (__cdecl *thrd_start_t)(void*);

int __cdecl _thrd_create_ex(thrd_t *thr, thrd_start_t func, void *arg, void *stack_bottom, size_t stack_size);
int __cdecl thrd_create(thrd_t *thr, thrd_start_t func, void *arg);

thrd_t __cdecl thrd_current(void);
int __cdecl thrd_detach(thrd_t thr);
int __cdecl thrd_equal(thrd_t lhs, thrd_t rhs);

__declspec(noreturn)
void __cdecl thrd_exit(int res);
int __cdecl thrd_join(thrd_t thr, int* res);
int __cdecl thrd_sleep(const struct timespec* duration, struct timespec* remaining);
void __cdecl thrd_yield(void);

void __cdecl mtx_destroy(mtx_t* mtx);
int __cdecl mtx_init(mtx_t* mtx, int type);
int __cdecl mtx_trylock(mtx_t* mtx);
int __cdecl mtx_lock(mtx_t* mtx);
int __cdecl mtx_unlock(mtx_t* mtx);

int __cdecl cnd_init(cnd_t* cond);
void __cdecl cnd_destroy(cnd_t* cond);
int __cdecl cnd_signal(cnd_t* cond);
int __cdecl cnd_broadcast(cnd_t* cond);
int __cdecl cnd_wait(cnd_t* cond, mtx_t* mtx);

int __cdecl tss_create(tss_t *tss_key, tss_dtor_t destructor);
int __cdecl tss_delete(tss_t tss_id);
int __cdecl tss_set(tss_t tss_id, void *val);
void __cdecl *tss_get(tss_t tss_key);

void __cdecl call_once(once_flag* flag, void(*_Func)(void));

int __cdecl _usem_init(struct _usem *sem, int initial_count, int max_count);
void __cdecl _usem_destroy(struct _usem *sem);
int __cdecl _usem_acquire(struct _usem *sem);
int __cdecl _usem_release(struct _usem *sem);


#endif
