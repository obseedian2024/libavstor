/*
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

#include "threads.h"

// These are private declarations used by the modules that don't
// need to be in threads.h

#if defined(_M_I86)
typedef USHORT APIRET;
#endif

struct _tld {
    thrd_t          thr;
    void*           *tls_data;
#if defined(__OS2__)
    struct _tld     *next_detach;
    cnd_t           cnd_exited;
    cnd_t           cnd_joined;
    mtx_t           mtx_exit;
    mtx_t           mtx_joined;
#if defined(_M_I86)
    void            *dyn_stack;
#else
    HEV             thread_event;
#endif
    int             exit_code;
    int             thread_state;    
#endif
};

extern once_flag    __init_stdthread_flag;
extern void         __init_stdthread(void);

#define call_once_init_stdthread() call_once(&__init_stdthread_flag, __init_stdthread)

#if defined(__OS2__)

#include <stddef.h>

#if defined(_M_I86)

extern struct _tld* NEAR    *ThrdData;
#define THREAD_DATA         (ThrdData[*_threadid])

#define acquire_mutex       DosSemRequest
#define release_mutex       DosSemClear
#define post_event          DosSemClear
#define reset_event         DosSemSet
#define wait_event          DosSemWait

static APIRET __inline wait_event_noint(PULONG sem, long timo)
{
    APIRET result;
    while (ERROR_INTERRUPT == (result = DosSemWait(sem, timo)))
        ;
    return result;
}

static APIRET __inline acquire_mutex_noint(PULONG sem, long timo)
{
    APIRET result;
    while (ERROR_INTERRUPT == (result = DosSemRequest(sem, timo)))
        ;
    return result;
}

#else

extern struct _tld*             *CurThrdData;
#define THREAD_DATA             (*CurThrdData)

#define acquire_mutex(m, timo)  DosRequestMutexSem(*(m), timo)
#define release_mutex(m)        DosReleaseMutexSem(*(m))
#define post_event(e)           DosPostEventSem(*(e))
#define reset_event(e)          _reset_event(*(e))
#define wait_event(e, timo)     DosWaitEventSem(*(e), timo)

static APIRET __inline _reset_event(HEV hEvent)
{
    ULONG post_cnt;
    return DosResetEventSem(hEvent, &post_cnt);
}

static APIRET __inline wait_event_noint(PHEV sem, long timo)
{
    APIRET result;
    while (ERROR_INTERRUPT == (result = DosWaitEventSem(*sem, timo)))
        ;
    return result;
}

static APIRET __inline acquire_mutex_noint(PHMTX sem, long timo)
{
    APIRET result;
    while (ERROR_INTERRUPT == (result = DosRequestMutexSem(*sem, timo)))
        ;
    return result;
}

#endif

#else

extern DWORD __key_tld;

#define THREAD_DATA  ((struct _tld *)TlsGetValue(__key_tld))

#endif
