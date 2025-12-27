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

#if defined(__OS2__)
#undef _WIN32

#define INCL_DOS
#define INCL_DOSERRORS
#define INCL_DOSPROCESS
#include <os2.h>

#include <malloc.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#elif defined(_WIN32)

#define WIN32_LEAN_AND_MEAN 1
#include <Windows.h>

#endif

#include <process.h>
#include <stdio.h>

#include "stdatomic.h"
#include "threads.h"

enum {
    tstate_running      = 0,
    tstate_exited       = 1,
    tstate_detached     = 2,
    tstate_joining      = 4,
    tstate_joined       = 8,
    tstate_die          = 16
};

struct _tld {
    thrd_t          thr;
#if defined(__OS2__)
    struct _tld     *next_detach;
    cnd_t           cnd_exited;
    cnd_t           cnd_joined;
    mtx_t           mtx_exit;
    mtx_t           mtx_joined;
    void            *dyn_stack;
    int             exit_code;
    int             thread_state;
    void            **tls_data;
#endif
};

static once_flag    init_stdthread_flag = ONCE_FLAG_INIT;
static struct _tld  tdata_main;


#if defined(_WIN32)

static tss_t key_tld;

struct ThreadParams {
    HANDLE          event_init;
    HANDLE          event_done;
    thrd_start_t    func;
    void            *arg;
    thrd_t          *thr;
    struct _tld     *tdata;
};

#define THREAD_DATA  ((struct _tld *)tss_get(key_tld))

static void init_stdthread(void)
{
    if (tss_create(&key_tld, NULL) != thrd_success) {
        fprintf(stderr, "FATAL: stdthrd: tss_create failed.\n");
        abort();
    }
    tdata_main.thr._ThreadID = GetCurrentThreadId();

    // Note: this is a pseudo-handle.
    tdata_main.thr._Handle = GetCurrentThread();

    if (tss_set(key_tld, &tdata_main) != thrd_success) {
        fprintf(stderr, "FATAL: stdthrd: tss_set failed.\n");
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

    WaitForSingleObject(param->event_init, INFINITE);

    tdata.thr = *param->thr;
    tss_set(key_tld, &tdata);

    SetEvent(param->event_done);

    return (unsigned)p_func(p_arg);
}

int __cdecl _thrd_create_ex(thrd_t *thr, thrd_start_t func, void *arg, void *stack_bottom, size_t stack_size)
{
    struct ThreadParams param;
    thrd_t l_thr;
    int result;

    (void)stack_bottom;

    call_once(&init_stdthread_flag, init_stdthread);

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

    *thr = l_thr;
    result = thrd_success;
finalize_and_return:
    CloseHandle(param.event_init);
    CloseHandle(param.event_done);
    return result;
}

int __cdecl thrd_equal(thrd_t lhs, thrd_t rhs)
{
    return lhs._ThreadID == rhs._ThreadID;
}

__declspec(noreturn)
void __cdecl thrd_exit(int res)
{
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

int __cdecl tss_create(tss_t *tss_key, tss_dtor_t destructor)
{
    unsigned key;

    key = TlsAlloc();
    // TODO: support TLS destructors
    (void)destructor;
    if (key == TLS_OUT_OF_INDEXES) {
        return thrd_error;
    }
    tss_key->_key = key;
    return thrd_success;
}

int __cdecl tss_delete(tss_t tss_id)
{
    return TlsFree(tss_id._key) ? thrd_success : thrd_error;
}

int __cdecl tss_set(tss_t tss_id, void *val)
{
    return TlsSetValue(tss_id._key, val) ? thrd_success : thrd_error;
}

void __cdecl *tss_get(tss_t tss_key)
{
    void *result = TlsGetValue(tss_key._key);
    if (!result && GetLastError() != ERROR_SUCCESS) {
        fprintf(stderr, "stdthrd: TlsGetValue failed.\n");
    }
    return result;
}


#elif defined(__OS2__)

#define MAX_TLS_KEY 255

extern unsigned     __MaxThreads;
size_t              __ThreadStackSize = 4096;

static 
struct _tld* NEAR   *ThrdData = NULL;

static ULONG        ThrdSem = 0;
static ULONG        ThrdInitSem = 0;
static thrd_start_t ThrdFunc;
static void         *ThrdStack;
static thrd_t       Thrd;

static mtx_t        mtx_detach;
static cnd_t        cnd_detach;
static thrd_t       detach_thread;
static void         *detach_thread_stack;
static struct _tld  *thread_data_list = NULL;

static signed char  TLSIndexMap[256] = { 0 };
static tss_dtor_t   TLSDestructors[256] = { NULL };
static mtx_t        mtx_tls;

#define THREAD_DATA (*get_tld())

static __inline struct _tld **get_tld(void)
{
    return &ThrdData[*_threadid];
}

// This must be called with ThrdSem owned
static int is_thrd_valid(thrd_t thr)
{
    USHORT prty;

    if (NO_ERROR != DosGetPrty(2, &prty, (USHORT)thr._thr_id)) {
        return 0;
    }
    return ThrdData[thr._thr_id] == thr._thr_data;
}

// tdata->mtx_joined must be held on entry
static void wait_for_thread_to_die(struct _tld *tdata)
{
    // Signal the thread to die
    tdata->thread_state |= tstate_die;

    // We need to suspend thread creation before we signal
    DosSemRequest(&ThrdSem, -1);
    cnd_signal(&tdata->cnd_joined);
    mtx_unlock(&tdata->mtx_joined);
    while (is_thrd_valid(tdata->thr)) {
        DosSleep(0);
    }

    // At this point the OS thread has ended, but its stack is still valid
    // This is because ThrdSem is held and no new thread can grab that memory

    // Clean up thread resources
    cnd_destroy(&tdata->cnd_exited);
    cnd_destroy(&tdata->cnd_joined);
    mtx_destroy(&tdata->mtx_exit);
    mtx_destroy(&tdata->mtx_joined);
    if (tdata->dyn_stack) {
        free(tdata->dyn_stack);
        tdata->dyn_stack = NULL;
    }

    // Thread data is now no longer valid
    ThrdData[tdata->thr._thr_id] = NULL;

    DosSemClear(&ThrdSem);
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

static void PASCAL FAR done_stdthread(USHORT termCode)
{
    (void)termCode;

    mtx_destroy(&mtx_detach);
    cnd_destroy(&cnd_detach);

    cnd_destroy(&tdata_main.cnd_exited);
    cnd_destroy(&tdata_main.cnd_joined);
    mtx_destroy(&tdata_main.mtx_exit);
    mtx_destroy(&tdata_main.mtx_joined);

    free(tdata_main.tls_data);
    free(detach_thread_stack);
    _nfree(ThrdData);

    mtx_destroy(&mtx_tls);
    DosExitList(EXLST_EXIT, NULL);
}

static void init_stdthread(void)
{
    DosSemRequest(&ThrdSem, -1);

    ThrdData = _ncalloc(__MaxThreads, sizeof(*ThrdData));
    if (!ThrdData) {
        fprintf(stderr, "FATAL: stdthrd: Failed to allocate thread data buffer\n");
        abort();
    }

    memset(&tdata_main, 0, sizeof(tdata_main));

    tdata_main.tls_data = calloc(MAX_TLS_KEY + 1, sizeof(void*));
    if (!tdata_main.tls_data) {
        fprintf(stderr, "FATAL: stdthrd: Failed to allocate TLS storage\n");
        abort();
    }

    tdata_main.thr._thr_data = &tdata_main;
    tdata_main.thr._thr_id = *_threadid;
    tdata_main.thread_state = tstate_running;

    THREAD_DATA = &tdata_main;

    ThrdData[tdata_main.thr._thr_id] = &tdata_main;

    mtx_init(&mtx_tls, mtx_plain);

    cnd_init(&tdata_main.cnd_exited);
    cnd_init(&tdata_main.cnd_joined);
    mtx_init(&tdata_main.mtx_exit, mtx_plain);
    mtx_init(&tdata_main.mtx_joined, mtx_plain);

    mtx_init(&mtx_detach, mtx_plain);
    cnd_init(&cnd_detach);

    if (!(detach_thread_stack = malloc(4096))) {
        fprintf(stderr, "FATAL: stdthrd: Failed to allocate detach-thread stack.\n");
        abort();
    }

    DosSemClear(&ThrdSem);

    if (_thrd_create_ex(&detach_thread, detach_thrdproc, NULL, detach_thread_stack, 4096) != thrd_success) {
        free(detach_thread_stack);
        fprintf(stderr, "FATAL: stdthrd: Failed to create detach-thread.\n");
        abort();
    }

    DosExitList(EXLST_ADD, done_stdthread);
}

static
void _WCCALLBACK
threadproc(void *arglist)
{
    struct _tld tdata;
    thrd_start_t p_func = ThrdFunc;

    memset(&tdata, 0, sizeof(tdata));
    tdata.tls_data = calloc(MAX_TLS_KEY + 1, sizeof(void*));
    if (!tdata.tls_data) {
        Thrd._thr_id = -1;
        fprintf(stderr, "stdthrd: Failed to allocate TLS storage\n");
        DosSemClear(&ThrdInitSem);
        _endthread();
    }

    tdata.dyn_stack = ThrdStack;
    tdata.thr._thr_data = &tdata;
    tdata.thr._thr_id = *_threadid;
    tdata.thread_state = tstate_running;
    
    THREAD_DATA = &tdata;
    Thrd._thr_data = &tdata;
    Thrd._thr_id = tdata.thr._thr_id;
    ThrdData[tdata.thr._thr_id] = &tdata;

    cnd_init(&tdata.cnd_exited);
    cnd_init(&tdata.cnd_joined);
    mtx_init(&tdata.mtx_exit, mtx_plain);
    mtx_init(&tdata.mtx_joined, mtx_plain);

    DosSemClear(&ThrdInitSem);

    // run the caller's thread proc and exit thread
    thrd_exit(p_func(arglist));
}

int __cdecl _thrd_create_ex(thrd_t *thr, thrd_start_t func, void *arg, void *stack_bottom, size_t stack_size)
{
    void *l_stack;
    int result, create_result;

    call_once(&init_stdthread_flag, init_stdthread);

    DosSemRequest(&ThrdSem, -1);

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
    ThrdFunc = func;
    DosSemSet(&ThrdInitSem);
    create_result = _beginthread(&threadproc, l_stack, (unsigned)stack_size, arg);

    if (create_result == -1) {
        if (ThrdStack) {
            free(ThrdStack);
            ThrdStack = NULL;
        }
        result = thrd_error;
        goto finalize_and_return;
    }

    DosSemWait(&ThrdInitSem, -1);
    if (Thrd._thr_id != -1) {
        *thr = Thrd;
        result = thrd_success;
    }
    else {
        result = thrd_error;
    }
finalize_and_return:
    DosSemClear(&ThrdSem);
    return result;
}


int __cdecl thrd_equal(thrd_t lhs, thrd_t rhs)
{
    return lhs._thr_id == rhs._thr_id && lhs._thr_data == rhs._thr_data;
}

__declspec(noreturn)
void __cdecl thrd_exit(int res)
{
    struct _tld *tdata = THREAD_DATA;
    unsigned i;

    tdata->exit_code = res;

    // Call TLS destructors
    mtx_lock(&mtx_tls);
    for (i = 0; i <= MAX_TLS_KEY; i++) {
        if (TLSIndexMap[i] && TLSDestructors[i]) {
            mtx_unlock(&mtx_tls);
            TLSDestructors[i](tdata->tls_data[i]);
            mtx_lock(&mtx_tls);
        }
    }

    // Free TLS data
    if (tdata->tls_data) {
        free(tdata->tls_data);
        tdata->tls_data = NULL;
    }
    mtx_unlock(&mtx_tls);

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

    _endthread();
}

int __cdecl thrd_detach(thrd_t thr)
{
    struct _tld *tdata;
    int result;

    DosSemRequest(&ThrdSem, -1);
    if (!is_thrd_valid(thr)) {
        DosSemClear(&ThrdSem);
        return thrd_error;
    }
    tdata = thr._thr_data;
    
    mtx_lock(&tdata->mtx_exit);
    if (!(tdata->thread_state & (tstate_joining | tstate_detached))) {
        DosSemClear(&ThrdSem);
        tdata->thread_state |= tstate_detached;
        cnd_signal(&tdata->cnd_joined);
        result = thrd_success;
    }
    else {
        DosSemClear(&ThrdSem);
        result = thrd_error;
    }
    mtx_unlock(&tdata->mtx_exit);

    return result;
}

int __cdecl thrd_join(thrd_t thr, int *res)
{
    struct _tld *tdata;

    DosSemRequest(&ThrdSem, -1);
    if (!is_thrd_valid(thr)) {
        DosSemClear(&ThrdSem);
        return thrd_error;
    }
    tdata = thr._thr_data;

    mtx_lock(&tdata->mtx_exit);
    if (!(tdata->thread_state & (tstate_detached | tstate_joining))) {
        DosSemClear(&ThrdSem);

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
        DosSemClear(&ThrdSem);
        mtx_unlock(&tdata->mtx_exit);
        return thrd_error;
    }
}

int __cdecl thrd_sleep(const struct timespec* duration, struct timespec* remaining)
{
    long ms = duration->tv_sec * 1000 + duration->tv_nsec / 1000000;
    USHORT result = DosSleep(ms) == NO_ERROR ? 0 : -1;
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

int __cdecl tss_create(tss_t *tss_key, tss_dtor_t destructor)
{
    unsigned i;
    int result = thrd_error;

    call_once(&init_stdthread_flag, init_stdthread);
    mtx_lock(&mtx_tls);
    for (i = 0; i <= MAX_TLS_KEY; i++) {
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
    if (tss_id._key > MAX_TLS_KEY) {
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
    if (tss_id._key > MAX_TLS_KEY) {
        return thrd_error;
    }
    THREAD_DATA->tls_data[tss_id._key] = val;
    return thrd_success;
}

void __cdecl *tss_get(tss_t tss_key)
{
    if (tss_key._key > MAX_TLS_KEY || !TLSIndexMap[tss_key._key]) {
        return NULL;
    }
    return THREAD_DATA->tls_data[tss_key._key];
}

#endif

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
    if (!_locked_load(_flag)) {
        if (!(_locked_exchange(_flag, 1)))
        {
            _func();
        }
    }
}
