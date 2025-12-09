/*
* Basic C11-style mutex and condition variable library
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

#if defined(__OS2__)
#undef _WIN32

#define INCL_DOS
#define INCL_DOSERRORS

#include <os2.h>

#endif

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN 1
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "stdatomic.h"
#include "threads.h"

// turn off nannying
#ifdef _MSC_VER
#pragma warning(disable:4996) // deprecated
#if defined(NDEBUG)
#pragma warning(disable:4100) // unreferenced parameter
#endif
#endif

#if defined(__clang__)
#define PRAGMA_LOOP_NO_UNROLL _Pragma("clang loop unroll(disable)")
#else 
#define PRAGMA_LOOP_NO_UNROLL
#endif

#define _MAX_SEM ((int)(((unsigned)-1) >> 1))

#if defined(_WIN32)
#include <Windows.h>

typedef union WaiterState {
    struct {
        short sema;
        short max_waiters;
    };
    int       state;
} WaiterState;

//
// User space counting semaphore. These will be significantly faster for
// the uncontended case, since we don't need to call into the kernel
//
int __cdecl _usem_init(struct _usem *sem, int initial_count, int max_count)
{
    memset(sem, 0, sizeof(*sem));
    sem->_max_count = max_count;
    sem->_sema_count = initial_count;
    InitializeCriticalSection(&sem->_cs);
    sem->_event = CreateEventA(NULL, FALSE, FALSE, NULL);
    if (!sem->_event) {
        DeleteCriticalSection(&sem->_cs);
        return 0;
    }
    return 1;
}

void __cdecl _usem_destroy(struct _usem *sem)
{
    DeleteCriticalSection(&sem->_cs);
    CloseHandle(sem->_event);
    memset(sem, 0, sizeof(*sem));
}

//
// Acquire semaphore. If we had to wait and the wait was not satisfied, 
// return 0 else return 1
//
int __cdecl _usem_acquire(struct _usem *sem)
{
    int result = 1;
    EnterCriticalSection(&sem->_cs);
    while (sem->_sema_count == 0) {
        DWORD wait_result;
        sem->_waiters++;
        LeaveCriticalSection(&sem->_cs);

        while ((wait_result = WaitForSingleObjectEx(sem->_event, INFINITE, TRUE)) == WAIT_IO_COMPLETION)
            ;
        if (wait_result != WAIT_OBJECT_0) {
            result = 0;
        }

        EnterCriticalSection(&sem->_cs);
        sem->_waiters--;
    }
    sem->_sema_count--;
    LeaveCriticalSection(&sem->_cs);
    return result;
}

int __cdecl _usem_release(struct _usem *sem)
{
    EnterCriticalSection(&sem->_cs);
    sem->_sema_count++;
    if (sem->_waiters > 0) {
        if (!SetEvent(sem->_event)) {
            LeaveCriticalSection(&sem->_cs);
            return 0;
        }
    }
    LeaveCriticalSection(&sem->_cs);
    return 1;
}

//
// Acquire condvar semaphore. If we need to wait, return 1 else 0.
//
static int __inline _cnd_acquire_sema(cnd_t *cond)
{
    WaiterState cur; 
    WaiterState next;
    cur.state = atomic_load(&cond->_state);
    do {
        next = cur;
        next.sema--;
        if (-next.sema > next.max_waiters) {
            next.max_waiters = -next.sema;
        }
    } while (!atomic_compare_exchange_weak(&cond->_state, &cur.state, next.state));

    return next.sema < 0;
}

//
// Release condvar semaphore. If we need to release the kernel semaphore, 
// return 1 else 0.
//
static int __inline _cnd_release_sema(cnd_t *cond)
{
    WaiterState cur;
    WaiterState next;
    cur.state = atomic_load(&cond->_state);
    while (cur.sema < cur.max_waiters) {
        next = cur;
        next.sema++;
        if (atomic_compare_exchange_weak(&cond->_state, &cur.state, next.state)) {
            return cur.sema < 0;
        }
    }
    return 0;
}

int __cdecl cnd_init(cnd_t* cond)
{
    WaiterState st;
    st.max_waiters = 1;
    st.sema = 0;

    if (!(cond->_ksem = CreateSemaphoreA(NULL, 0, 0x7FFFFFFF, NULL))) {
        return thrd_error;
    }
    atomic_store(&cond->_state, st.state);
    return thrd_success;
}

void __cdecl cnd_destroy(cnd_t* cond)
{
    CloseHandle(cond->_ksem);
}

int __cdecl cnd_wait(cnd_t* cond, mtx_t* mtx)
{
    int result = thrd_success;
    int should_wait = _cnd_acquire_sema(cond);

    if (mtx_unlock(mtx) != thrd_success) {
        return thrd_error;
    }

    if (should_wait) {
        DWORD wait_result = WaitForSingleObjectEx(cond->_ksem, INFINITE, TRUE);

        // If the wait is not satisified, we need to release the condvar semaphore
        if (wait_result != WAIT_OBJECT_0) {
            _cnd_release_sema(cond);
        }

        result = (wait_result == WAIT_OBJECT_0 || wait_result == WAIT_IO_COMPLETION)
            ? thrd_success : thrd_error;
    }

    if (mtx_lock(mtx) != thrd_success) {
        return thrd_error;
    }
    return result;
}

int __cdecl cnd_signal(cnd_t* cond)
{
    if (_cnd_release_sema(cond)) {
        return ReleaseSemaphore(cond->_ksem, 1, NULL) ? thrd_success : thrd_error;
    }
    return thrd_success;
}

int __cdecl cnd_broadcast(cnd_t* cond)
{
    WaiterState cur;
    WaiterState next;
    cur.state = atomic_load(&cond->_state);
    while (cur.sema < cur.max_waiters) {
        next = cur;
        next.sema = next.max_waiters;
        if (atomic_compare_exchange_weak(&cond->_state, &cur.state, next.state)) {
            if (cur.sema < 0) {
                return ReleaseSemaphore(cond->_ksem, -cur.sema, NULL) ? thrd_success : thrd_error;
            }
            break;
        }
    }
    return thrd_success;
}

#elif defined(__OS2__)

static USHORT _DosSemWait(HSEM sem, long timo)
{
    USHORT result;
    while (ERROR_INTERRUPT == (result = DosSemWait(sem, timo)))
        ;
    return result;
}

static USHORT _DosSemRequest(HSEM sem, long timo)
{
    USHORT result;
    while (ERROR_INTERRUPT == (result = DosSemRequest(sem, timo)))
        ;
    return result;
}

int _usem_init(struct _usem *sem, int initial_count, int max_count)
{
    memset(sem, 0, sizeof(*sem));
    sem->_max_count = max_count;
    sem->_sema_count = initial_count;
    return 1;
}

void _usem_destroy(struct _usem *sem)
{
    memset(sem, 0, sizeof(*sem));
}

int _usem_acquire(struct _usem *sem)
{
    if (_DosSemRequest(&sem->_mtx_sem, -1) != NO_ERROR) {
        return 0;
    }
	while (sem->_sema_count == 0) {
		sem->_waiters++;
        DosSemSet(&sem->_event_sem);
		DosSemClear(&sem->_mtx_sem);

        if (_DosSemWait(&sem->_event_sem, -1) != NO_ERROR) {
            return 0;
        }

        if (_DosSemRequest(&sem->_mtx_sem, -1) != NO_ERROR) {
            return 0;
        }
		sem->_waiters--;
	}
	sem->_sema_count--;
    DosSemClear(&sem->_mtx_sem);
    return 1;
}

int _usem_release(struct _usem *sem)
{
    if (_DosSemRequest(&sem->_mtx_sem, -1) != NO_ERROR) {
        return 0;
    }
    sem->_sema_count++;
    if (sem->_waiters > 0) {
        DosSemClear(&sem->_event_sem);
    }
    DosSemClear(&sem->_mtx_sem);
    return 1;
}

int __cdecl cnd_init(cnd_t* cond)
{
    memset(cond, 0, sizeof(*cond));
    cond->_max_waiters = 1;
    mtx_init(&cond->_mtx, mtx_plain);
    return thrd_success;
}

void __cdecl cnd_destroy(cnd_t *cond)
{
    mtx_destroy(&cond->_mtx);
    memset(cond, 0, sizeof(*cond));
}

static int _cnd_try_enqueue_wait_item(cnd_t *cond, struct _wait_item *item)
{
    int should_wait;

    cond->_sema_count--;
    if (-cond->_sema_count > cond->_max_waiters) {
        cond->_max_waiters = -cond->_sema_count;
    }

    if ((should_wait = cond->_sema_count < 0)) {
        item->_next = NULL;
        item->_pred = cond->_tail;
        item->_wait_sema = 0;
        if (cond->_tail) {            
            cond->_tail->_next = item;
            cond->_tail = item;
        }
        else {
            cond->_tail = item;
            cond->_head = item;
        }
        DosSemSet(&item->_wait_sema);
    }
    return should_wait;
}

static void _cnd_remove_wait_item(cnd_t *cond, struct _wait_item *item)
{
    if (cond->_head) {
        if (cond->_head == item) {
            cond->_head = item->_next;
        }
        if (cond->_tail == item) {
            cond->_tail = item->_pred;
        }
        if (item->_next) {
            item->_next->_pred = item->_pred;
        }
        if (item->_pred) {
            item->_pred->_next = item->_next;
        }
    }
    item->_next = NULL;
    item->_pred = NULL;
}

static struct _wait_item *_cnd_dequeue_wait_item(cnd_t *cond)
{
    struct _wait_item *result = cond->_head;
    if (result) {
        if (result->_next) {
            result->_next->_pred = NULL;
        }
        if (!(cond->_head = result->_next)) {
            cond->_tail = NULL;
        }
        result->_next = NULL;
        result->_pred = NULL;
    }
    return result;
}

int __cdecl cnd_wait(cnd_t *cond, mtx_t *mtx)
{
    struct _wait_item item;
    int result = thrd_success;
    int should_wait;

    mtx_lock(&cond->_mtx);
    should_wait = _cnd_try_enqueue_wait_item(cond, &item);
    mtx_unlock(&cond->_mtx);

    mtx_unlock(mtx);

    if (should_wait) {
        USHORT wait_result = DosSemWait(&item._wait_sema, -1);
        switch (wait_result) {
        case NO_ERROR:
        case ERROR_INTERRUPT:
            result = thrd_success;
            break;
        case ERROR_SEM_TIMEOUT:
            result = thrd_timedout;
            break;
        default:
            result = thrd_error;
        }
        // If the wait was not satisfied, remove ourselves from the queue
        if (wait_result != NO_ERROR) {
            mtx_lock(&cond->_mtx);
            _cnd_remove_wait_item(cond, &item);
            mtx_unlock(&cond->_mtx);
        }
    }
    mtx_lock(mtx);
    return result;
}

int __cdecl cnd_signal(cnd_t* cond)
{
    struct _wait_item *item = NULL;

    mtx_lock(&cond->_mtx);
    if (cond->_sema_count < cond->_max_waiters) {
        cond->_sema_count++;
        if (cond->_sema_count <= 0) {
            if ((item = _cnd_dequeue_wait_item(cond))) {
                DosSemClear(&item->_wait_sema);
            }
        }
    }
    mtx_unlock(&cond->_mtx);
    return thrd_success;
}

int __cdecl cnd_broadcast(cnd_t *cond)
{
    struct _wait_item *item;

    mtx_lock(&cond->_mtx);
    cond->_sema_count = 0;

    while ((item = _cnd_dequeue_wait_item(cond))) {
        DosSemClear(&item->_wait_sema);
    }

    mtx_unlock(&cond->_mtx);
    return thrd_success;
}

#endif

int __cdecl mtx_init(mtx_t* mtx, int type)
{
    if (type & mtx_recursive) {
        fprintf(stderr, "FATAL: stdthrd: Recursive mutexes are not currently supported.\n");
        abort();
    }
    mtx->_type = type;
    _locked_store(&mtx->_lock, 0);
    _locked_store(&mtx->_count, 0);
    return _usem_init(&mtx->_wait_sem, 0, _MAX_SEM) ? thrd_success : thrd_error;
}

void __cdecl mtx_destroy(mtx_t* mtx)
{
    _usem_destroy(&mtx->_wait_sem);
}

int __cdecl mtx_trylock(mtx_t* mtx)
{
    return atomic_exchange(&mtx->_lock, 1) ? thrd_error : thrd_success;
}

// This is pretty fast if compiled with clang and real C11 atomics
// OK on Watcom
int __cdecl mtx_lock(mtx_t* mtx)
{
    while (atomic_exchange(&mtx->_lock, 1)) {
        if (atomic_fetch_add(&mtx->_count, -1) <= 0) {
            if (!_usem_acquire(&mtx->_wait_sem)) {
                atomic_fetch_add(&mtx->_count, 1);
                return thrd_error;
            }
        }
    }
    return thrd_success;
}

int __cdecl mtx_unlock(mtx_t* mtx)
{
    if (atomic_exchange(&mtx->_lock, 0)) {
        int cur = atomic_load(&mtx->_count);

        // This has to be a <= comparison, not < as it might be expected.
        // Otherwise, deadlocks can occur.
        while (cur <= 0) {
            if (atomic_compare_exchange_strong(&mtx->_count, &cur, cur + 1)) {
                if (cur < 0 && !_usem_release(&mtx->_wait_sem)) {
                    return thrd_error;
                }
                break;
            }
        }
        return thrd_success;
	}
    return thrd_error;
}
