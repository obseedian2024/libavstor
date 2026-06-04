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

#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "stdatomic.h"
#include "_threads.h"

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

#define _MAX_SEM            ((short)(((unsigned short)-1) >> 1))
#define MCS_LOCK_SPIN       40

// TODO: more robust checking of return values
// TODO: cache-friendliness

int __cdecl _mtx_init(mtx_t *mtx, int type);

//
// An MCS parking lock
//
static void mcs_lock_acquire(struct _mcs_lock *ml, struct _mcs_node *ctx)
{
	struct _mcs_node *pred;
	atomic_store_ptr(&ctx->next, NULL);
	pred = atomic_exchange_ptr(&ml->_tail, ctx);

	if (pred != NULL) {
        int spin = MCS_LOCK_SPIN;
        int one = 1;
		atomic_store(&ctx->locked, 1);
		atomic_store_ptr(&pred->next, ctx);
        while (atomic_load(&ctx->locked) && spin-- > 0) {
            _cpu_pause();
        }

        // Atomically update lock state to waiting only if still locked
        if (atomic_compare_exchange_strong(&ctx->locked, &one, -1))
        {
            wait_event(ctx->event, -1);
            reset_event(ctx->event);

            assert(atomic_load(&ctx->locked) == 0);
        }
	}
}

static void mcs_lock_release(struct _mcs_lock *ml, struct _mcs_node *ctx)
{
    struct _mcs_node *next;

	if ((next = atomic_load_ptr(&ctx->next)) == NULL) {
		struct _mcs_node *cur = ctx;
		if (atomic_compare_exchange_ptr_strong(&ml->_tail, &cur, NULL)) {
			return;
		}
		while ((next = atomic_load_ptr(&ctx->next)) == NULL) {
			_cpu_pause();
		}
	}
    
    // Set event only if definitely waiting
    if (atomic_exchange(&next->locked, 0) < 0) {
        post_event(next->event);
    }
}

//
// User space counting semaphore. These will be significantly faster for
// the uncontended case, since we don't need to call into the kernel
//
int __cdecl _usem_init(struct _usem *sem, short initial_count, short max_count)
{
    memset(sem, 0, sizeof(*sem));
    sem->_max_count = max_count;
    sem->_sema_count = initial_count;
#if defined(_WIN32)
    sem->_event = CreateEventA(NULL, TRUE, FALSE, NULL);
    if (!sem->_event) {
        return 0;
    }
#elif defined(__OS2__)
#if defined(_M_I86)
    sem->_event = 0;
    reset_event(&sem->_event);
#else
    if (NO_ERROR != DosCreateEventSem(NULL, &sem->_event, 0, FALSE)) {
        fprintf(stderr, "stdthrd: DosCreateEventSem failed.\n");
        return 0;
    }
#endif
#endif
    return 1;
}

void __cdecl _usem_destroy(struct _usem *sem)
{
#if defined(_WIN32)
    CloseHandle(sem->_event);
#elif defined(__OS2__) && !defined(_M_I86)
    DosCloseEventSem(sem->_event);
#endif
    memset(sem, 0, sizeof(*sem));
}

//
// Acquire semaphore. If we had to wait and the wait was not satisfied, 
// return 0 else return 1
//
int __cdecl _usem_acquire(struct _usem *sem)
{
    int result = 1;
    struct _mcs_node *mnode = &THREAD_DATA->sem_lock;
    
    if (!wait_event(&sem->_event, -1)) return 0;

    mcs_lock_acquire(&sem->_lock, mnode);
    while (sem->_sema_count == 0) {
        mcs_lock_release(&sem->_lock, mnode);

        if (!wait_event(&sem->_event, -1)) return 0;

        mcs_lock_acquire(&sem->_lock, mnode);
    }
    sem->_sema_count--;
    if (sem->_sema_count == 0) {
        reset_event(&sem->_event);
	}
    mcs_lock_release(&sem->_lock, mnode);

    return result;
}

int __cdecl _usem_release(struct _usem *sem)
{
    struct _mcs_node *mnode = &THREAD_DATA->sem_lock;

    mcs_lock_acquire(&sem->_lock, mnode);
    if (sem->_sema_count < sem->_max_count) {
        if (sem->_sema_count == 0) {
            post_event(&sem->_event);
        }
        sem->_sema_count++;
    }
    mcs_lock_release(&sem->_lock, mnode);

    return 1;
}

/* 
* This simple condition variable implementation is basically a counting
* semaphore with a dynamically adjusted upper limit. The number of waiters
* is the semaphore count and the number of wakeups is the upper limit.
* Not compliant with POSIX semantics since wakeups can be stolen. Fairness
* depends on underlying OS event implementation: reasonably fair under NT
* but very unfair under OS/2.
*/
int __cdecl _cnd_init(cnd_t* cond)
{
    cond->_waiters = 0;
    cond->_wakeups = 0;
    if (thrd_error == _mtx_init(&cond->_mtx, mtx_plain)) {
        return thrd_error;
    }
#if defined(_WIN32)
    if (!(cond->_event = CreateEventA(NULL, TRUE, FALSE, NULL))) {
        mtx_destroy(&cond->_mtx);
        return thrd_error;
    }
#elif defined(__OS2__)
#if defined(_M_I86)
    cond->_event = 0;
    reset_event(&cond->_event);
#else
    if (NO_ERROR != DosCreateEventSem(NULL, &cond->_event, 0, FALSE)) {
        fprintf(stderr, "stdthrd: DosCreateEventSem failed.\n");
        mtx_destroy(&cond->_mtx);
        return thrd_error;
    }
#endif
#endif
    return thrd_success;
}

int __cdecl cnd_init(cnd_t *cond)
{
    call_once_init_stdthread();
    return _cnd_init(cond);
}

void __cdecl cnd_destroy(cnd_t* cond)
{
    mtx_destroy(&cond->_mtx);
#if defined(_WIN32)
    CloseHandle(cond->_event);
#elif defined(__OS2__) && !defined(_M_I86)
    DosCloseEventSem(cond->_event);
#endif
}

static void _cnd_decrement_waiters(cnd_t *cond)
{
    mtx_lock(&cond->_mtx);

    // Decrement waiters and ensure wakeups is not more than waiters
    cond->_waiters--;
    if (cond->_wakeups > cond->_waiters) {
        cond->_wakeups = cond->_waiters;
    }

    mtx_unlock(&cond->_mtx);
}

int __cdecl cnd_wait(cnd_t *cond, mtx_t *mtx)
{
    //
    // It does not seem advantageous to wait when _wakeups > 0 since 
    // we are using a manual reset event and it would still be posted, hence
    // the wait would be immediately satisfied anyway
    //
    mtx_lock(&cond->_mtx);
    while (cond->_wakeups == 0) {
        int wait_succ;
        cond->_waiters++;
		mtx_unlock(&cond->_mtx);

        if (mtx_unlock(mtx) != thrd_success) {
            _cnd_decrement_waiters(cond);
            return thrd_error;
        }
        
        wait_succ = wait_event(&cond->_event, -1);

        if (mtx_lock(mtx) != thrd_success) {
            _cnd_decrement_waiters(cond);
            return thrd_error;
        }

		mtx_lock(&cond->_mtx);
        cond->_waiters--;

        if (!wait_succ) {
            mtx_unlock(&cond->_mtx);
            return thrd_error;
        }
    }

    cond->_wakeups--;

    // If all wakeups satisfied, block further waiters.
    if (cond->_wakeups == 0) {
        reset_event(&cond->_event);
        }
	mtx_unlock(&cond->_mtx);

    return thrd_success;
}

int __cdecl cnd_signal(cnd_t* cond)
{
	mtx_lock(&cond->_mtx);    
    if (cond->_wakeups < cond->_waiters) {
        if (cond->_wakeups == 0) {
            post_event(&cond->_event);
        }
        cond->_wakeups++;
    }
	mtx_unlock(&cond->_mtx);

    return thrd_success;
}

int __cdecl cnd_broadcast(cnd_t* cond)
{
	mtx_lock(&cond->_mtx);    
    if (cond->_wakeups < cond->_waiters) {
        if (cond->_wakeups == 0) {
            post_event(&cond->_event);
        }
        cond->_wakeups = cond->_waiters;
    }
	mtx_unlock(&cond->_mtx);

    return thrd_success;
}

int __cdecl _mtx_init(mtx_t* mtx, int type)
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

int __cdecl mtx_init(mtx_t *mtx, int type)
{
    call_once_init_stdthread();
    return _mtx_init(mtx, type);
}

void __cdecl mtx_destroy(mtx_t* mtx)
{
    _usem_destroy(&mtx->_wait_sem);
}

int __cdecl mtx_trylock(mtx_t* mtx)
{
#if defined(_M_IX86) && _M_IX86 == 300
    return _locked_exchange(&mtx->_lock, 1) ? thrd_error : thrd_success;
#else
    return atomic_exchange(&mtx->_lock, 1) ? thrd_error : thrd_success;
#endif
}

int __cdecl mtx_lock(mtx_t* mtx)
{
#if defined(_M_IX86) && _M_IX86 == 300
    // Special version for 386 so we don't have to use global spinlock
    while (_locked_exchange(&mtx->_lock, 1)) {
        if (_locked_dec(&mtx->_count) < 0) {
            if (!_usem_acquire(&mtx->_wait_sem)) {
                _locked_inc(&mtx->_count);
                return thrd_error;
            }
        }
    }
#else
    while (atomic_exchange(&mtx->_lock, 1)) {
        if (atomic_fetch_add(&mtx->_count, -1) <= 0) {
            if (!_usem_acquire(&mtx->_wait_sem)) {
                atomic_fetch_add(&mtx->_count, 1);
                return thrd_error;
            }
        }
    }
#endif
    return thrd_success;
}

int __cdecl mtx_unlock(mtx_t* mtx)
{
#if defined(_M_IX86) && _M_IX86 == 300
    if (_locked_exchange(&mtx->_lock, 0)) {
        if (_locked_load(&mtx->_count) <= 0) {
            if (_locked_inc(&mtx->_count) <= 0 && !_usem_release(&mtx->_wait_sem)) {
                return thrd_error;
            }
        }
        return thrd_success;
    }
#else
    if (atomic_exchange(&mtx->_lock, 0)) {
        if (atomic_load(&mtx->_count) <= 0) {
            if (atomic_fetch_add(&mtx->_count, 1) < 0 && !_usem_release(&mtx->_wait_sem)) {
                return thrd_error;
            }
        }       
        return thrd_success;
	}
#endif
    return thrd_error;
}
