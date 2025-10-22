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

#define _CRT_INTERNAL_NONSTDC_NAMES 1

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#if defined(_WIN32)
#include <Windows.h>

#if !defined(ENABLE_VIRTUAL_TERMINAL_PROCESSING)
#define ENABLE_VIRTUAL_TERMINAL_PROCESSING 0x0004
#endif
#elif defined(__unix__)
#include <unistd.h>
#else 

#endif 

#if !defined(__unix__)
#include <io.h>
#include <conio.h>
#endif

#include <avstor.h>

#include "avstest.h"
#include "timer.h"

#ifdef _MSC_VER
#pragma warning(disable:4996) // deprecated
#endif 

char* RED = "\033[1;31m";
char* GRN = "\033[1;32m";
char* YEL = "\033[1;33m";
char* WHT = "\033[1;37m";
char* CRESET = "\033[0m";

static void show_result(const char *descr, int result, double duration)
{
    char buf[50];
    size_t len_descr = 0;
    const size_t SZBUF_M1 = sizeof(buf) - 1;

    memset(&buf, '.', SZBUF_M1);
    buf[SZBUF_M1] = 0;
    if (descr) {
        len_descr = strlen(descr);
        memcpy(buf, descr, len_descr > SZBUF_M1 ? SZBUF_M1 : len_descr);
    }
    printf("%s [ %s%s%s ] [ %s%12.4f%s ]\n", buf, result ? GRN : RED,
           result ? "PASS" : "FAIL", CRESET, WHT, duration, CRESET);
}

int avstest_run_test(const AvsTest *test, double *duration)
{
    Timer tm;
    int result;

    printf("Running %s...\n", test->test_name);
    timer_start(&tm);
    result = test->test_fn(test->params);
    timer_stop(&tm);

    *duration = tm.secs;
    show_result(NULL, result, tm.secs);
    return result;
}

static int run_all_tests(const AvsTests* *tests, double *total_duration)
{
    int i;
    int result = (0 == 0);

    *total_duration = 0.0;
    while (*tests) {
        double duration = 0.0;
        int file_result = (0 == 0);
        printf("--> Running tests in %s\n", (*tests)->test_file);

        for (i = 0; i < (*tests)->test_count; i++) {
            int test_result = avstest_run_test(&(*tests)->test_list[i], &duration);
            file_result = file_result && test_result;
            *total_duration += duration;
            if (!test_result && ((*tests)->test_list[i].flags & AVSTEST_MUST_PASS)) {
                printf("%sPrevious test marked as MUST PASS, stopping tests.%s\n\n",YEL, CRESET);
                return 0;
            }
        }

        printf("===========================================================================\n");
        show_result((*tests)->test_file, file_result, duration);
        printf("\n");
        result = result && file_result;
        tests++;
    }
    return result;
}

#if !defined(__unix__)

/* Clear console buffer */
static void kb_clear(void)
{
    while (kbhit()) {
        getch();
    }
}

static int check_tty(void)
{
    kb_clear();
    if (!cputs("\033[6n")) {
        if (kbhit()) {
            if (getch() == 27) {
                kb_clear();
                return 1;
            }            
        }        
        cputs("\015");
    }
    return 0;
}
#endif

#if defined(_WIN32)

static int is_terminal(void)
{
    HANDLE hConsole = GetStdHandle(STD_OUTPUT_HANDLE);
    DWORD mode;
    if (hConsole == INVALID_HANDLE_VALUE) {
        return 0;
    }
    if (!GetConsoleMode(hConsole, &mode)) {
        fprintf(stderr, "GetConsoleMode failed\n");
        return 0;
    }
    if (mode & ENABLE_VIRTUAL_TERMINAL_PROCESSING) {
        return 1;
    }
    else if (mode & ENABLE_PROCESSED_OUTPUT) {
        return check_tty();
    }
    return 0;
}
#elif defined(__unix__)
static int is_terminal(void)
{
    return isatty(STDOUT_FILENO);
}
#else

static int is_terminal(void)
{
    if (!isatty(STDOUT_FILENO)) {
        return 0;
    }
    return check_tty();
}
#endif

/* check if we're running under a terminal, and if not, disable color codes */
static int init_term(void)
{
    if (is_terminal()) {
        fputs(CRESET, stdout);
        return 1;
    }
    RED = "";
    GRN = "";
    WHT = "";
    YEL = "";
    CRESET = "";
    return 0;
}

int is_term;

IMPORT_TESTS(DFS);

static const AvsTests* ALL_TESTS[] = {
    &DFS_TESTS,
    NULL
};

int main(void)
{
    double duration;
    int result;

    is_term = init_term();

    result = run_all_tests(ALL_TESTS, &duration);

    show_result("ALL TESTS", result, duration);
   
    return result ? 0 : 1;
}
