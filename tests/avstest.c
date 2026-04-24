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
#endif 

#if !defined(_WIN32)
#include <sys/stat.h>
#endif

#if !defined(__unix__)
#include <io.h>
#include <conio.h>
#endif

#include "avstest.h"
#include "timer.h"

#ifdef _MSC_VER
#pragma warning(disable:4996) // deprecated
#endif 

static const char *AVS_TARGET_ARCH =
#if defined(__amd64) || defined(__amd64__) || defined(__x86_64) || defined(__x86_64__)
"x86_64"
#elif defined(__I86__) || defined(M_I86) || defined(_M_I86)
#if defined(__OS2__)
"i286"
#else
"i8086"
#endif
#elif defined(i386) || defined(__i386) || defined(_M_IX86)
"i386"
#elif defined(__alpha__) || defined(__alpha) || defined(__M_ALPHA) || defined(_M_ALPHA)
"alpha"
#elif defined(_M_MRX000) || defined(__mips__) || defined(__mips) || defined(__MIPS__) || defined(mips)
"mips"
#if (defined(__POINTER_WIDTH__) && __POINTER_WIDTH__ == 64)
"64"
#endif
#if (defined(__LITTLE_ENDIAN__) && __LITTLE_ENDIAN__ == 1) || defined(_WIN32)
"el"
#endif
#elif defined(__aarch64__)
"aarch64"
#elif defined(__arm__) || defined(_M_ARM) || defined(__arm)
"arm"
#elif defined(__m68k__)
"m68k"
#elif defined(__powerpc64__) || defined(__ppc64__) || defined(__PPC64__) || defined(_ARCH_PPC64) || defined(_ARCH_PPC64)
"powerpc64"
#if (defined(__LITTLE_ENDIAN__) && __LITTLE_ENDIAN__ == 1) || defined(_WIN32)
"le"
#endif
#elif defined(__powerpc) || defined(__powerpc__) || defined(__POWERPC__) || defined(__ppc__) \
    || defined(__PPC__) || defined(_M_PPC) || defined(__M_PPC) || defined(__ppc)
"powerpc"
#else
"unknown"
#endif
"-"
#if defined(_WIN32)
"win32"
#elif defined(__OS2__)
"os2"
#elif defined(__DOS__) || defined(_DOS) || defined(MSDOS)
"msdos"
#elif defined(__FreeBSD__)
"freebsd"
#elif defined(__linux__)
"linux"
#else
"unknown"
#endif
;

const char* vtDSR = "\033[6n"; //Device status sequence, also works on ANSI.SYS

char* vtRED = "\033[1;31m";
char* vtGRN = "\033[1;32m";
char* vtYEL = "\033[1;33m";
char* vtCYN = "\033[1;36m";
char* vtWHT = "\033[1;37m";
char* vtCRESET = "\033[0m";  //Reset color

static void show_result(const char *descr, int result, double duration)
{
    char buf[50];
    const size_t SZBUF_M1 = sizeof(buf) - 1;

    memset(&buf, '.', SZBUF_M1);
    buf[SZBUF_M1] = 0;
    if (descr) {
        const size_t len_descr = strlen(descr);
        memcpy(buf, descr, len_descr > SZBUF_M1 ? SZBUF_M1 : len_descr);
    }
    printf("%s [ %s%s%s ] [ %s%12.4f%s ]\n", buf, result ? vtGRN : vtRED,
           result ? "PASS" : "FAIL", vtCRESET, vtWHT, duration, vtCRESET);
}

static void show_perfstat(int64_t node_total, int64_t bytes_total, double duration)
{
    double kbytes_per_sec;
    char buf_bytes[30];
    if (node_total <= 0 || bytes_total <= 0 || duration <= 0) {
        return;
    }
    kbytes_per_sec = (double)bytes_total / (duration * 1024.0);
    if (kbytes_per_sec < 1000.0) {
        sprintf(buf_bytes, "%.3f KB/s", kbytes_per_sec);
    }
    else if (kbytes_per_sec < 1000000.0) {
        sprintf(buf_bytes, "%.3f MB/s", kbytes_per_sec / 1024.0);
    }
    else {
        sprintf(buf_bytes, "%.3f GB/s", kbytes_per_sec / (1024.0 * 1024.0));
    }
    printf("...Processed %s%.0f%s nodes/s (%s%s%s)\n", vtWHT, (double)node_total / duration,
        vtCRESET, vtWHT, buf_bytes, vtCRESET);
}

int avstest_run_test(const AvsTest *test, double *duration)
{
    Timer tm;
    int result;
    int64_t nodes_total, bytes_total;
    printf("Running %s...\n", test->test_name);
    timer_start(&tm);
    result = test->test_fn(test->params, &nodes_total, &bytes_total);
    timer_stop(&tm);

    *duration = tm.secs;

    show_perfstat(nodes_total, bytes_total, tm.secs);
    show_result(NULL, result, tm.secs);
    return result;
}

static int run_all_tests(const AvsTests* *tests, double *total_duration)
{
    int i;
    int result = (0 == 0);

    *total_duration = 0.0;
    while (*tests) {
        double file_duration = 0.0;
        int file_result = (0 == 0);
        printf("--> Running tests in %s\n", (*tests)->test_file);

        for (i = 0; i < (*tests)->test_count; i++) {
            if ((*tests)->test_list[i].flags & AVSTEST_SKIP) {
                printf("%sSkipping %s.%s\n", vtYEL, (*tests)->test_list[i].test_name, vtCRESET);
            }
            else {
                double duration;
                int test_result = avstest_run_test(&(*tests)->test_list[i], &duration);
                file_result = file_result && test_result;
                file_duration += duration;
                *total_duration += duration;
                if (!test_result && ((*tests)->test_list[i].flags & AVSTEST_MUST_PASS)) {
                    printf("%sPrevious test marked as MUST PASS, stopping tests.%s\n\n", vtYEL, vtCRESET);
                    return 0;
                }
            }
        }

        printf("===========================================================================\n");
        show_result((*tests)->test_file, file_result, file_duration);
        printf("\n");
        result = result && file_result;
        tests++;
    }
    return result;
}

#if defined(_WIN32)

// 
// Customer Win32 immplementation since C libraries seem wonky for terminal detection
// especially for Borland C++ 5.0
//
static int is_terminal(void)
{
    HANDLE hOutput = GetStdHandle(STD_OUTPUT_HANDLE);
    DWORD mode;
    if (hOutput == INVALID_HANDLE_VALUE) {
        return 0;
    }
    if (!GetConsoleMode(hOutput, &mode)) {
        return 0;
    }
    if (mode & ENABLE_VIRTUAL_TERMINAL_PROCESSING) {
        return 1;
    }
    if (mode & ENABLE_PROCESSED_OUTPUT) {        
        HANDLE hInput = GetStdHandle(STD_INPUT_HANDLE);
        if (hInput == INVALID_HANDLE_VALUE) {
            return 0;
        }
        if (!GetConsoleMode(hInput, &mode)) {
            return 0;
        }
        if (mode & ENABLE_PROCESSED_INPUT) {
            DWORD num;

            // Make sure no existing keyboard input to interfere with us
            FlushConsoleInputBuffer(hInput);

            // Query device status
            if (!WriteConsoleA(hOutput, vtDSR, (DWORD)strlen(vtDSR), &num, NULL)) {
                return 0;
            }

            // Check if there is a response
            if (GetNumberOfConsoleInputEvents(hInput, &num) && num > 0) {
                INPUT_RECORD rec;

                // get the first character
                if (ReadConsoleInputA(hInput, &rec, 1, &num) && num > 0) {
                    // If the terminal response starts with ESC, we can assume it's a terminal
                    if (rec.Event.KeyEvent.uChar.AsciiChar == '\033') {
                         FlushConsoleInputBuffer(hInput);
                         return 1;
                    }
                }
            }
            WriteConsoleA(hOutput, "\r", 1, &num, NULL);
            FlushConsoleInputBuffer(hInput);
        }
    }
    return 0;
}
#elif defined(__unix__)
static int is_terminal(void)
{
    return isatty(STDOUT_FILENO);
}
#else

/* Clear console buffer */
static void kb_clear(void)
{
    while (kbhit()) {
        getch();
    }
}

static int is_terminal(void)
{
    if (!isatty(STDOUT_FILENO)) {
        return 0;
    }
    kb_clear();
    if (!cputs(vtDSR)) {
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

/* check if we're running under a terminal, and if not, disable color codes */
static int init_term(void)
{
    if (is_terminal()) {
        fputs(vtCRESET, stdout);
        return 1;
    }
    vtRED = "";
    vtGRN = "";
    vtWHT = "";
    vtYEL = "";
    vtCYN = "";
    vtCRESET = "";
    return 0;
}

int64_t avstest_getfilesize(const char *filename)
{
#if defined(_WIN32)
    WIN32_FIND_DATAA fdata;
    HANDLE hSearch;
    int64_t filesize;
    ZeroMemory(&fdata, sizeof(WIN32_FIND_DATAA));
    if (INVALID_HANDLE_VALUE == (hSearch = FindFirstFileA(filename, &fdata))) {
        return -1;
    }
    filesize = ((int64_t)fdata.nFileSizeHigh << 32) | fdata.nFileSizeLow;
    FindClose(hSearch);
    return filesize;
#else
    struct stat fdata;
    memset(&fdata, 0, sizeof(struct stat));
    if (stat(filename, &fdata)) {
        return -1;
    }
    return (int64_t)fdata.st_size;
#endif
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

    printf("libavstor Test Suite\n"
           "BSD 3-Clause License\n"
           "Copyright (c) 2025 Tamas Fejerpataky\n"
           "See project at https://github.com/obseedian2024/libavstor\n");
    printf("Target: %s%s%s\n\n", vtCYN, AVS_TARGET_ARCH, vtCRESET);

    result = run_all_tests(ALL_TESTS, &duration);

    show_result("ALL TESTS", result, duration);
   
    return result ? 0 : 1;
}
