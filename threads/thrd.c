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

#include <malloc.h>

#if defined(__OS2__)
#undef _WIN32

#define INCL_DOS
#define INCL_DOSERRORS
#define INCL_DOSPROCESS
#include <os2.h>

#include <string.h>

#elif defined(_WIN32)

#define WIN32_LEAN_AND_MEAN 1
#include <Windows.h>

#endif

#include <process.h>
#include <stdio.h>
#include <stdlib.h>

#include "stdatomic.h"
#include "_threads.h"

enum {
    tstate_running      = 0,
    tstate_exited       = 1,
    tstate_detached     = 2,
    tstate_joining      = 4,
    tstate_joined       = 8,
    tstate_die          = 16
};

once_flag           __init_stdthread_flag = ONCE_FLAG_INIT;
static struct _tld  tdata_main;

static signed char  TLSIndexMap[_MAX_TLS_KEY + 1] = { 0 };
static tss_dtor_t   TLSDestructors[_MAX_TLS_KEY + 1] = { NULL };
static mtx_t        mtx_tls;

extern int __cdecl _cnd_init(cnd_t *cond);
extern int __cdecl _mtx_init(mtx_t *mtx, int type);

static int create_thread_event(struct _tld *tdata)
{
#if defined(_WIN32)
    tdata->thread_event = CreateEventA(NULL, TRUE, FALSE, NULL);
    if (tdata->thread_event == NULL) {
        fprintf(stderr, "stdthrd: CreateEvent failed.\n");
        return 0;
    }
#elif defined(__OS2__)
#if defined(_M_I86)
    tdata->thread_event = 0;
    reset_event(&tdata->thread_event);
#else
    if (NO_ERROR != DosCreateEventSem(NULL, &tdata->thread_event, 0, FALSE)) {
        fprintf(stderr, "stdthrd: DosCreateEventSem failed.\n");
        return 0;
    }
#endif
#endif

    tdata->sem_lock.event = &tdata->thread_event;
    return 1;
}

static void destroy_thread_event(struct _tld *tdata)
{
#if defined(_WIN32)
    CloseHandle(tdata->thread_event);
#elif defined(__OS2__) 
#if defined(_M_I86)
    tdata->thread_event = 0;
#else
    DosCloseEventSem(tdata->thread_event);
#endif
#endif
}

static void *alloc_tls_data(struct _tld* tdata)
{
    tdata->tls_data = calloc(_MAX_TLS_KEY + 1, sizeof(void *));
    if (!tdata->tls_data) {
        fprintf(stderr, "stdthrd: Failed to allocate TLS storage\n");
    }
    return tdata->tls_data;
}

static void finalize_tls_data(struct _tld* tdata)
{
    int i;

    mtx_lock(&mtx_tls);
    if (tdata->tls_data) {
        // Call TLS destructors        
        for (i = 0; i <= _MAX_TLS_KEY; i++) {
            if (TLSIndexMap[i] && TLSDestructors[i]) {
                mtx_unlock(&mtx_tls);
                TLSDestructors[i](tdata->tls_data[i]);
                mtx_lock(&mtx_tls);
            }
        }

        // Free TLS data    
        free(tdata->tls_data);
        tdata->tls_data = NULL;
    }
    mtx_unlock(&mtx_tls);
}

#if defined(_WIN32)

DWORD __key_tld;

struct ThreadParams {
    HANDLE          event_init;
    HANDLE          event_done;
    thrd_start_t    func;
    void            *arg;
    thrd_t          *thr;
    struct _tld     *tdata;
};

typedef int *pint;
typedef void *pvoid;

#if defined(__WATCOMC__)
static void _WCCALLBACK done_stdthread(void)
#else
static void __cdecl done_stdthread(void)
#endif
{   
    finalize_tls_data(&tdata_main);
    mtx_destroy(&mtx_tls);
    destroy_thread_event(&tdata_main);
    TlsFree(__key_tld);
}

void __init_stdthread(void)
{
    ZeroMemory(&tdata_main, sizeof(tdata_main));
    tdata_main.thr._ThreadID = GetCurrentThreadId();
    
    // Note: this is a pseudo-handle.
    tdata_main.thr._Handle = GetCurrentThread();    

    if (!create_thread_event(&tdata_main)) {
        abort();
    }

    _mtx_init(&mtx_tls, mtx_plain);

    if (TLS_OUT_OF_INDEXES == (__key_tld = TlsAlloc())) {
        fprintf(stderr, "FATAL: stdthrd: TlsAlloc failed.\n");
        abort();
    }

    if (!alloc_tls_data(&tdata_main)) {
        abort();
    }    

    TlsSetValue(__key_tld, &tdata_main);

    if (atexit(done_stdthread)) {
        fprintf(stderr, "FATAL: stdthrd: atexit failed.\n");
        abort();
    }
}

static
unsigned __stdcall
threadproc(void *arglist)
{       
    struct ThreadParams *param = (struct ThreadParams *)arglist;
    struct _tld tdata;

    thrd_start_t p_func = param->func;
    void *p_arg = param->arg;

    memset(&tdata, 0, sizeof(tdata));

    if (!create_thread_event(&tdata)) {
        goto err_event;
    }

    if (!alloc_tls_data(&tdata)) {
        goto err_tls_alloc;
    }

    WaitForSingleObject(param->event_init, INFINITE);

    tdata.thr = *param->thr;
    TlsSetValue(__key_tld, &tdata);

    SetEvent(param->event_done);
    thrd_exit(p_func(p_arg));

err_tls_alloc:
    destroy_thread_event(&tdata);
err_event:
    param->thr->_Handle = NULL;
    SetEvent(param->event_done);
    _endthread();

    // We shoould never reach this point
    abort();
    return 0;
}

int __cdecl _thrd_create_ex(thrd_t *thr, thrd_start_t func, void *arg, void *stack_bottom, size_t stack_size)
{
    struct ThreadParams param;
    thrd_t l_thr;
    int result;

    (void)stack_bottom;

    call_once_init_stdthread();

    if (!(param.event_init = CreateEventA(NULL, FALSE, 0, NULL))) {
        return thrd_error;
    }
    if (!(param.event_done = CreateEventA(NULL, FALSE, 0, NULL))) {
        CloseHandle(param.event_init);
        return thrd_error;
    }
    param.func = func;
    param.arg = arg;
    param.thr = &l_thr;

    l_thr._Handle = (void *)_beginthreadex(NULL, stack_size, &threadproc, &param, 0, &l_thr._ThreadID);
    if (l_thr._Handle == NULL) {
        result = thrd_error;
        goto finalize_and_return;
    }
    SetEvent(param.event_init);
    WaitForSingleObject(param.event_done, INFINITE);

    if (param.thr->_Handle) {
        *thr = l_thr;
        result = thrd_success;
    }
    else {
        result = thrd_error;
    }
finalize_and_return:
    CloseHandle(param.event_init);
    CloseHandle(param.event_done);
    return result;
}

int __cdecl thrd_equal(thrd_t lhs, thrd_t rhs)
{
    return lhs._ThreadID == rhs._ThreadID;
}

NORETURN
void __cdecl thrd_exit(int res)
{
    struct _tld *tdata = THREAD_DATA;
    finalize_tls_data(tdata);
    destroy_thread_event(tdata);
    _endthreadex((DWORD)res);

    // To prevent compiler warning since _endthreadex not marked as noreturn
    for (;;)
        ;
}

int __cdecl thrd_detach(thrd_t thr)
{
    if (thr._Handle) {
        return CloseHandle(thr._Handle) ? thrd_success : thrd_error;
    }
    return thrd_error;
}

int __cdecl thrd_join(thrd_t thr, int *res)
{
    DWORD result;
    while (WAIT_IO_COMPLETION == (result = WaitForSingleObjectEx(thr._Handle, INFINITE, TRUE)))
        ;
    if (result != WAIT_OBJECT_0) {
        return thrd_error;
    }
    if (GetExitCodeThread(thr._Handle, (LPDWORD)res)) {
        CloseHandle(thr._Handle);
        return thrd_success;
    }
    else {
        return thrd_error;
    }
}

int __cdecl thrd_sleep(const struct timespec *duration, struct timespec *remaining)
{
    long ms = duration->tv_sec * 1000 + duration->tv_nsec / 1000000;
    DWORD result = SleepEx((DWORD)ms, TRUE);
    if (result == WAIT_IO_COMPLETION) {
        if (remaining != NULL) {
            // TODO: Fix this
            remaining->tv_sec = 0;
            remaining->tv_nsec = 0;
        }
        return -1;
    }
    else if (result == 0) {
        if (remaining != NULL) {
            remaining->tv_sec = 0;
            remaining->tv_nsec = 0;
        }
        return 0;
    }
    return -2;
}

void __cdecl thrd_yield(void)
{
#if _WIN32_WINNT >= 0x0400
    SwitchToThread();
#endif
}

#elif defined(__OS2__)

extern unsigned     __MaxThreads;
#if defined(_M_I86)
size_t              __ThreadStackSize = 4096;
#else
size_t              __ThreadStackSize = 8192;
#endif

struct _tld* NEAR   *ThrdData = NULL;

static thrd_start_t ThrdFunc;
static thrd_t       Thrd;

static mtx_t        mtx_detach;
static cnd_t        cnd_detach;
static thrd_t       detach_thread;
static struct _tld  *thread_data_list = NULL;
static ULONG        ThrdInitSem = 0;
static ULONG        ThrdSem = 0;

#if defined(_M_I86)
static void         *ThrdStack;
static void         *detach_thread_stack;
#else
struct _tld*        *CurThrdData;
#endif

// This must be called with ThrdSem owned
static int is_thrd_valid(thrd_t thr)
{
#if defined(_M_I86)
    USHORT prty;    
    if (NO_ERROR != DosGetPrty(2, &prty, (USHORT)thr._thr_id)) {
        return 0;
    }
#else
    TID thr_id = (TID)thr._thr_id;
    APIRET res;
    while (ERROR_INTERRUPT == (res = DosWaitThread(&thr_id, DCWW_NOWAIT)))
        ;
    if (res == ERROR_INVALID_THREADID) {
        return 0;
    }
#endif
    return ThrdData[thr._thr_id] == thr._thr_data;
}

// tdata->mtx_joined must be held on entry
static void wait_for_thread_to_die(struct _tld *tdata)
{
#if !defined(_M_I86)
    TID thr_id = (TID)tdata->thr._thr_id;
#endif
    // Signal the thread to die
    tdata->thread_state |= tstate_die;
    
    // We need to suspend thread creation before we signal
    acquire_mutex(&ThrdSem, -1);
    cnd_signal(&tdata->cnd_joined);
    mtx_unlock(&tdata->mtx_joined);
#if defined(_M_I86)
    while (is_thrd_valid(tdata->thr)) {
        DosSleep(0);
    }

    // At this point the OS thread has ended, but its stack is still valid
    // This is because ThrdSem is held and no new thread can grab that memory
    if (tdata->dyn_stack) {
        free(tdata->dyn_stack);
        tdata->dyn_stack = NULL;
    }

    // Thread data is now no longer valid
    ThrdData[tdata->thr._thr_id] = NULL;
#else
    while (ERROR_INTERRUPT == DosWaitThread(&thr_id, DCWW_WAIT))
        ;    
#endif

    release_mutex(&ThrdSem);
}

// Asynchronously cleans up detached threads
static int __cdecl detach_thrdproc(void *arg)
{
    (void)arg;
    while (1) {
        mtx_lock(&mtx_detach);
        while (!thread_data_list) {
            cnd_wait(&cnd_detach, &mtx_detach);
        }
        while (thread_data_list) {
            struct _tld *tdata = thread_data_list;
            thread_data_list = thread_data_list->next_detach;
            mtx_unlock(&mtx_detach);

            mtx_lock(&tdata->mtx_joined);
            wait_for_thread_to_die(tdata);

            mtx_lock(&mtx_detach);
        }
        mtx_unlock(&mtx_detach);
    }
}

static void done_stdthread(void)
{
    mtx_destroy(&mtx_detach);
    cnd_destroy(&cnd_detach);

    cnd_destroy(&tdata_main.cnd_exited);
    cnd_destroy(&tdata_main.cnd_joined);
    mtx_destroy(&tdata_main.mtx_exit);
    mtx_destroy(&tdata_main.mtx_joined);

    //free(tdata_main.tls_data);
    finalize_tls_data(&tdata_main);
#if defined(_M_I86)
    free(detach_thread_stack);
#else
    DosFreeThreadLocalMemory((PULONG)CurThrdData);
    CurThrdData = NULL;
#endif
    _nfree(ThrdData);

    mtx_destroy(&mtx_tls);
    destroy_thread_event(&tdata_main);
}

static
void _WCCALLBACK
threadproc(void *arglist)
{
    struct _tld tdata;
    thrd_start_t p_func = ThrdFunc;

    memset(&tdata, 0, sizeof(tdata));
    if (!alloc_tls_data(&tdata)) {
        Thrd._thr_id = -1;
        post_event(&ThrdInitSem);
        _endthread();
    }

#if defined(_M_I86)   
    tdata.dyn_stack = ThrdStack;
#endif
    if (!create_thread_event(&tdata)) {
        Thrd._thr_id = -1;
        fprintf(stderr, "stdthrd: Failed to create thread event\n");
        free(tdata.tls_data);
        post_event(&ThrdInitSem);
        _endthread();
    }
    tdata.thr._thr_data = &tdata;
    tdata.thr._thr_id = *_threadid;
    tdata.thread_state = tstate_running;
    
    THREAD_DATA = &tdata;
    Thrd._thr_data = &tdata;
    Thrd._thr_id = tdata.thr._thr_id;
    ThrdData[tdata.thr._thr_id] = &tdata;

    _cnd_init(&tdata.cnd_exited);
    _cnd_init(&tdata.cnd_joined);
    _mtx_init(&tdata.mtx_exit, mtx_plain);
    _mtx_init(&tdata.mtx_joined, mtx_plain);

    post_event(&ThrdInitSem);
    // run the caller's thread proc and exit thread
    thrd_exit(p_func(arglist));
}

static int __cdecl __thrd_create_ex(thrd_t *thr, thrd_start_t func, void *arg, void *stack_bottom, size_t stack_size)
{
    void *l_stack;
    int result, create_result;

    acquire_mutex(&ThrdSem, -1);

#if defined(_M_I86)
    if (!stack_bottom) {
        if (!stack_size) {
            stack_size = __ThreadStackSize;
        }
        if (!(ThrdStack = malloc(stack_size))) {
            result = thrd_error;
            goto finalize_and_return;
        }
        l_stack = ThrdStack;
    }
    else {
        ThrdStack = NULL;
        l_stack = stack_bottom;
    }
#else
    (void)stack_bottom;
    l_stack = NULL;
    if (!stack_size) {
        stack_size = __ThreadStackSize;
    }
#endif

    ThrdFunc = func;
    reset_event(&ThrdInitSem);
    create_result = _beginthread(&threadproc, l_stack, (unsigned)stack_size, arg);

    if (create_result == -1) {
#if defined(_M_I86)
        if (ThrdStack) {
            free(ThrdStack);
            ThrdStack = NULL;
        }
#endif
        result = thrd_error;
        goto finalize_and_return;
    }

    wait_event(&ThrdInitSem, -1);
    if (Thrd._thr_id != -1) {
        *thr = Thrd;
        result = thrd_success;
    }
    else {
        result = thrd_error;
    }
finalize_and_return:
    release_mutex(&ThrdSem);
    return result;
}

void __init_stdthread(void)
{
#if !defined(_M_I86)
    if (NO_ERROR != DosCreateMutexSem(NULL, &ThrdSem, 0, TRUE)) {
        fprintf(stderr, "FATAL: stdthrd: Failed to create thread mutex\n");
        abort();
    }
    if (NO_ERROR != DosCreateEventSem(NULL, &ThrdInitSem, 0, FALSE)) {
        fprintf(stderr, "FATAL: stdthrd: Failed to create thread event\n");
        abort();
    }
#else
    acquire_mutex(&ThrdSem, 1);
#endif
    ThrdData = _ncalloc(__MaxThreads, sizeof(*ThrdData));
    if (!ThrdData) {
        fprintf(stderr, "FATAL: stdthrd: Failed to allocate thread data buffer\n");
        abort();
    }

    memset(&tdata_main, 0, sizeof(tdata_main));

    if (!create_thread_event(&tdata_main)) {
        abort();
    }

    if (!alloc_tls_data(&tdata_main)) {
        destroy_thread_event(&tdata_main);
        abort();
    }

    tdata_main.thr._thr_data = &tdata_main;
    tdata_main.thr._thr_id = *_threadid;
    tdata_main.thread_state = tstate_running;
#if !defined(_M_I86)
    if (NO_ERROR != DosAllocThreadLocalMemory(1, (PULONG*)&CurThrdData)) {
        fprintf(stderr, "FATAL: stdthrd: Failed to allocate thread local memory\n");
        abort();
    }
#endif
    THREAD_DATA = &tdata_main;

    ThrdData[tdata_main.thr._thr_id] = &tdata_main;

    _mtx_init(&mtx_tls, mtx_plain);

    _cnd_init(&tdata_main.cnd_exited);
    _cnd_init(&tdata_main.cnd_joined);
    _mtx_init(&tdata_main.mtx_exit, mtx_plain);
    _mtx_init(&tdata_main.mtx_joined, mtx_plain);

    _mtx_init(&mtx_detach, mtx_plain);
    _cnd_init(&cnd_detach);
#if defined(_M_I86)
    if (!(detach_thread_stack = malloc(4096))) {
        fprintf(stderr, "FATAL: stdthrd: Failed to allocate detach-thread stack.\n");
        abort();
    }
#endif
    release_mutex(&ThrdSem);

    if (__thrd_create_ex(&detach_thread, detach_thrdproc, NULL,
#if defined(_M_I86)
        detach_thread_stack,
#else
        NULL,
#endif
        __ThreadStackSize) != thrd_success) {
#if defined(_M_I86)
        free(detach_thread_stack);
#endif
        fprintf(stderr, "FATAL: stdthrd: Failed to create detach-thread.\n");
        abort();
    }

    if (atexit(done_stdthread)) {
        fprintf(stderr, "FATAL: stdthrd: atexit failed.\n");
        abort();
    }
}

int __cdecl _thrd_create_ex(thrd_t *thr, thrd_start_t func, void *arg, void *stack_bottom, size_t stack_size)
{
    call_once_init_stdthread();
    return __thrd_create_ex(thr, func, arg, stack_bottom, stack_size);
}

int __cdecl thrd_equal(thrd_t lhs, thrd_t rhs)
{
    return lhs._thr_id == rhs._thr_id && lhs._thr_data == rhs._thr_data;
}

__declspec(noreturn)
void __cdecl thrd_exit(int res)
{
    struct _tld *tdata = THREAD_DATA;

    tdata->exit_code = res;    

    // signal thread_join that the thread has exited and the exit_code can be picked up
    mtx_lock(&tdata->mtx_exit);
    tdata->thread_state |= tstate_exited;
    cnd_signal(&tdata->cnd_exited);
    mtx_unlock(&tdata->mtx_exit);

    // synchronize with thread_join or thread_detach
    mtx_lock(&tdata->mtx_joined);
    while (!(tdata->thread_state & (tstate_joined | tstate_detached))) {
        cnd_wait(&tdata->cnd_joined, &tdata->mtx_joined);
    }
    mtx_unlock(&tdata->mtx_joined);

    // If detaching, add ourselves to cleanup queue
    if (tdata->thread_state & tstate_detached) {
        mtx_lock(&mtx_detach);
        tdata->next_detach = thread_data_list;
        thread_data_list = tdata;
        cnd_signal(&cnd_detach);
        mtx_unlock(&mtx_detach);
    }

    // Wait until allowed to die
    mtx_lock(&tdata->mtx_joined);
    while (!(tdata->thread_state & tstate_die)) {
        cnd_wait(&tdata->cnd_joined, &tdata->mtx_joined);
    }
    mtx_unlock(&tdata->mtx_joined);

    // Clean up thread resources
    cnd_destroy(&tdata->cnd_exited);
    cnd_destroy(&tdata->cnd_joined);
    mtx_destroy(&tdata->mtx_exit);
    mtx_destroy(&tdata->mtx_joined);

    finalize_tls_data(tdata);
    destroy_thread_event(tdata);
#if !defined(_M_I86)
    THREAD_DATA = NULL;
    tdata->thr._thr_data = NULL;
    ThrdData[tdata->thr._thr_id] = NULL;
#endif

    _endthread();
}

int __cdecl thrd_detach(thrd_t thr)
{
    struct _tld *tdata;
    int result;

    acquire_mutex(&ThrdSem, -1);
    if (!is_thrd_valid(thr)) {
        release_mutex(&ThrdSem);
        return thrd_error;
    }
    tdata = thr._thr_data;
    
    mtx_lock(&tdata->mtx_exit);
    if (!(tdata->thread_state & (tstate_joining | tstate_detached))) {
        release_mutex(&ThrdSem);
        tdata->thread_state |= tstate_detached;
        cnd_signal(&tdata->cnd_joined);
        result = thrd_success;
    }
    else {
        release_mutex(&ThrdSem);
        result = thrd_error;
    }
    mtx_unlock(&tdata->mtx_exit);

    return result;
}

int __cdecl thrd_join(thrd_t thr, int *res)
{
    struct _tld *tdata;

    acquire_mutex(&ThrdSem, -1);
    if (!is_thrd_valid(thr)) {
        release_mutex(&ThrdSem);
        return thrd_error;
    }
    tdata = thr._thr_data;

    mtx_lock(&tdata->mtx_exit);
    if (!(tdata->thread_state & (tstate_detached | tstate_joining))) {
        release_mutex(&ThrdSem);

        // Tell thrd_detach that we are joining
        tdata->thread_state |= tstate_joining;

        // Wait until thread exit code is available
        while (!(tdata->thread_state & tstate_exited)) {
            cnd_wait(&tdata->cnd_exited, &tdata->mtx_exit);
        }
        *res = tdata->exit_code;
        mtx_unlock(&tdata->mtx_exit);

        // Let thread know we have joined
        mtx_lock(&tdata->mtx_joined);
        tdata->thread_state |= tstate_joined;
        
        // Signal thread to die and wait until it has really exited
        wait_for_thread_to_die(tdata);

        return thrd_success;
    }
    else {
        release_mutex(&ThrdSem);
        mtx_unlock(&tdata->mtx_exit);
        return thrd_error;
    }
}

int __cdecl thrd_sleep(const struct timespec* duration, struct timespec* remaining)
{
    long ms = duration->tv_sec * 1000 + duration->tv_nsec / 1000000;
    unsigned result = DosSleep(ms) == NO_ERROR ? 0 : -1;
    if (result && remaining) {
        // TODO: Fix me
        remaining->tv_sec = 0;
        remaining->tv_nsec = 0;
    }
    return result;
}

void __cdecl thrd_yield(void)
{
    _cpu_pause();
}

#endif

int __cdecl tss_create(tss_t *tss_key, tss_dtor_t destructor)
{
    unsigned i;
    int result = thrd_error;
 
    call_once_init_stdthread();

    mtx_lock(&mtx_tls);
    for (i = 0; i <= _MAX_TLS_KEY; i++) {
        if (!TLSIndexMap[i]) {
            TLSIndexMap[i] = -1;
            tss_key->_key = i;
            TLSDestructors[i] = destructor;
            result = thrd_success;
            break;
        }
    }
    mtx_unlock(&mtx_tls);

    return result;
}

int __cdecl tss_delete(tss_t tss_id)
{
    if (tss_id._key > _MAX_TLS_KEY) {
        return thrd_error;
    }

    mtx_lock(&mtx_tls);
    if (TLSIndexMap[tss_id._key] == 0) {
        mtx_unlock(&mtx_tls);
        return thrd_error;
    }
    TLSIndexMap[tss_id._key] = 0;
    TLSDestructors[tss_id._key] = NULL;
    mtx_unlock(&mtx_tls);

    return thrd_success;
}

int __cdecl tss_set(tss_t tss_id, void *val)
{
    if (tss_id._key > _MAX_TLS_KEY) {
        return thrd_error;
    }
    THREAD_DATA->tls_data[tss_id._key] = val;
    return thrd_success;
}

void* __cdecl tss_get(tss_t tss_key)
{
    if (tss_key._key > _MAX_TLS_KEY || !TLSIndexMap[tss_key._key]) {
        return NULL;
    }
    return THREAD_DATA->tls_data[tss_key._key];
}

int __cdecl thrd_create(thrd_t *thr, thrd_start_t func, void *arg)
{
    return _thrd_create_ex(thr, func, arg, NULL, 0);
}

thrd_t __cdecl thrd_current(void)
{
    return THREAD_DATA->thr;
}

void __cdecl call_once(once_flag *_flag, void(*_func)(void))
{
    int cur = 0;
    if (atomic_compare_exchange_strong(_flag, &cur, -1)) {
        _func();
        atomic_store(_flag, 1);
    }
    else if (cur < 0) {
        while (atomic_load(_flag) < 0) {
            const struct timespec dur = { 0 };
            thrd_sleep(&dur, NULL);
        }
    }
}
