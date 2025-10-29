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
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

#include "timer.h"

#if defined(_WIN32)
#define WIN32_LEAN_AND_MEAN 1
#include <Windows.h>
#else 
#include <time.h>
#endif

void timer_start(Timer *t)
{
    memset(t, 0, sizeof(*t));
#if defined(_WIN32)
    if (QueryPerformanceCounter(&t->pc_start_time))
    {
        t->flags |= TIMER_HIRES;
        return;
    }
    else {
        printf("WARNING: High resolution counter not available, falling back to clock().\n");
    }
#elif defined(__unix__)
    if (!clock_gettime(CLOCK_MONOTONIC, &t->ts_start_time)) {
        t->flags |= TIMER_HIRES;
        t->clock_id = CLOCK_MONOTONIC;
        return;
    }
    else if (!clock_gettime(CLOCK_REALTIME, &t->ts_start_time)) {
        printf("WARNING: CLOCK_MONOTONIC not available, falling back to CLOCK_REALTIME.\n");
        t->flags |= TIMER_HIRES;
        t->clock_id = CLOCK_REALTIME;
        return;
    }
    else {
        printf("WARNING: clock_gettime() failed, falling back to clock().\n");
    }
#endif
    t->start_time = clock();
}

void timer_stop(Timer *t)
{
#if defined(_WIN32)
    LARGE_INTEGER freq;
#elif defined(__unix__)
    struct timespec diff;
#endif
    if (t->flags & TIMER_HIRES) {
#if defined(_WIN32)
        QueryPerformanceCounter(&t->pc_end_time);
        if (!QueryPerformanceFrequency(&freq)) {
            abort();
        }
        t->secs = ((double)(t->pc_end_time.QuadPart - t->pc_start_time.QuadPart)) / (double)freq.QuadPart;
#elif defined(__unix__)
        clock_gettime(t->clock_id, &t->ts_end_time);
        diff.tv_nsec = t->ts_end_time.tv_nsec - t->ts_start_time.tv_nsec;
        diff.tv_sec = t->ts_end_time.tv_sec - t->ts_start_time.tv_sec;
        if (diff.tv_nsec < 0) {
            diff.tv_sec--;
            diff.tv_nsec += 1000000000l;
        }
        t->secs = (double)diff.tv_sec + (double)diff.tv_nsec / 1000000000.0;
#endif
        return;
    }
    t->end_time = clock();
    t->secs = (double)(t->end_time - t->start_time) / CLOCKS_PER_SEC;
}
