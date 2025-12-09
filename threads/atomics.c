/*
* Atomic functions for 16-bit x86 and 32-bit 386
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

#include "stdatomic.h"

#if _M_IX86 < 400

#if _M_IX86 < 300

static volatile char AtomicLockState = 0;

#define ACQUIRE_SPINLOCK(lock_addr) \
    __asm mov al, 1 \
    __asm __again: \
    __asm xchg al, byte ptr lock_addr \
    __asm test al, al \
    __asm je __acquired \
    __asm __spin: \
    __asm cmp byte ptr lock_addr, 0 \
    __asm jne __spin \
    __asm jmp __again \
    __asm __acquired:

// It is left as an exercise to the reader why lock xchg is not needed here
#define RELEASE_SPINLOCK(lock_addr) \
    __asm mov byte ptr lock_addr, 0

#else

static volatile int AtomicLockState = 1;

#if defined(__I86__)
#define LOCK_SIZE word
#else
#define LOCK_SIZE dword
#endif

// Slightly more efficient version for 386+ 
// Works if there are less than 2^31 (2^15 for 16-bit) threads spinning
#define ACQUIRE_SPINLOCK(lock_addr) \
    __asm __again: \
    __asm lock dec LOCK_SIZE ptr lock_addr \
    __asm jz __acquired \
    __asm __spin: \
    __asm cmp LOCK_SIZE ptr lock_addr, 0 \
    __asm jle __spin \
    __asm jmp __again \
    __asm __acquired:

#define RELEASE_SPINLOCK(lock_addr) \
    __asm mov LOCK_SIZE ptr lock_addr, 1

#endif

#if defined(__I86__)

int __cdecl
__atomic_fetch_add_impl(volatile atomic_int *obj, const int value)
{
    int result = 0;
    __asm {
        lds si, obj
        mov cx, value
        mov ax, SEG AtomicLockState
        mov bx, OFFSET AtomicLockState
        mov es, ax

        ACQUIRE_SPINLOCK(es:[bx])

        mov ax, [si]
        add cx, ax
        mov [si], cx
        mov result, ax

        RELEASE_SPINLOCK(es:[bx])
    }
    return result;
}

signed char __cdecl
__atomic_compare_exchange_impl(volatile atomic_int *obj, int *expected, const int desired)
{
    signed char result = 1;
    __asm {
        lds si, expected
        mov cx, desired
        mov ax, SEG AtomicLockState
        mov bx, OFFSET AtomicLockState
        mov es, ax

        ACQUIRE_SPINLOCK(es:[bx])

        les di, obj
        mov ax, [si]
        mov dx, es:[di]
        cmp dx, ax
        jne not_equal
        mov es:[di], cx
        jmp done
      not_equal:
        mov [si], dx
        mov result, 0
      done:

        mov si, SEG AtomicLockState
        mov es, si
        RELEASE_SPINLOCK(es:[bx])
    }
    return result;
}

void __cdecl
__atomic_store_impl(volatile atomic_int *obj, const int value)
{
    __asm {
        lds si, obj
        mov cx, value
        mov ax, SEG AtomicLockState
        mov bx, OFFSET AtomicLockState
        mov es, ax

        ACQUIRE_SPINLOCK(es:[bx])

        mov [si], cx

        RELEASE_SPINLOCK(es:[bx])
    }
}

int __cdecl
__atomic_load_impl(const volatile atomic_int *obj)
{
    int result = 0;
    __asm {
        lds si, obj
        mov ax, SEG AtomicLockState
        mov bx, OFFSET AtomicLockState
        mov es, ax

        ACQUIRE_SPINLOCK(es:[bx])

        mov ax, [si]
        mov result, ax

        RELEASE_SPINLOCK(es:[bx])
    }
    return result;
}

int __cdecl
__atomic_exchange_impl(volatile atomic_int *obj, const int value)
{
    int result = 0;
    __asm {
        lds si, obj
        mov cx, value
        mov ax, SEG AtomicLockState
        mov bx, OFFSET AtomicLockState
        mov es, ax

        ACQUIRE_SPINLOCK(es:[bx])

        mov ax, [si]
        mov [si], cx
        mov result, ax

        RELEASE_SPINLOCK(es:[bx])
    }
    return result;
}

int __cdecl
__atomic_add_impl(volatile atomic_int *obj, const int value)
{
    int result = 0;
    __asm {
        lds si, obj
        mov cx, value
        mov ax, SEG AtomicLockState
        mov bx, OFFSET AtomicLockState
        mov es, ax

        ACQUIRE_SPINLOCK(es:[bx])

        mov ax, [si]
        add ax, cx
        mov [si], ax
        mov result, ax

        RELEASE_SPINLOCK(es:[bx])
    }
    return result;
}

int __cdecl
__atomic_inc_impl(volatile atomic_int *obj)
{
    int result = 0;
    __asm {
        lds si, obj
        mov ax, SEG AtomicLockState
        mov bx, OFFSET AtomicLockState
        mov es, ax

        ACQUIRE_SPINLOCK(es:[bx])

        mov ax, [si]
        inc ax
        mov [si], ax
        mov result, ax

        RELEASE_SPINLOCK(es:[bx])
    }
    return result;
}

int __cdecl
__atomic_dec_impl(volatile atomic_int *obj)
{
    int result = 0;
    __asm {
        lds si, obj
        mov ax, SEG AtomicLockState
        mov bx, OFFSET AtomicLockState
        mov es, ax

        ACQUIRE_SPINLOCK(es:[bx])

        mov ax, [si]
        dec ax
        mov [si], ax
        mov result, ax

        RELEASE_SPINLOCK(es:[bx])
    }
    return result;
}

signed char __cdecl
__atomic_bit_test_and_set(volatile atomic_int *obj, unsigned num)
{
    signed char result = 1;
    __asm {
        lds si, obj
        mov ax, 1
        mov cx, num
        shl ax, cl
        mov cx, ax
        mov ax, SEG AtomicLockState
        mov bx, OFFSET AtomicLockState
        mov es, ax

        ACQUIRE_SPINLOCK(es:[bx])

        mov ax, [si]
        test ax, cx
        jnz bit_set
        mov result, 0
      bit_set:
        or ax, cx
        mov [si], ax

        RELEASE_SPINLOCK(es:[bx])
    }
    return result;
}

signed char __cdecl
__atomic_bit_test_and_clear(volatile atomic_int *obj, unsigned num)
{
    signed char result = 1;
    __asm {
        lds si, obj
        mov ax, 1
        mov cx, num
        shl ax, cl
        mov cx, ax
        mov ax, SEG AtomicLockState
        mov bx, OFFSET AtomicLockState
        mov es, ax

        ACQUIRE_SPINLOCK(es:[bx])

        mov ax, [si]
        test ax, cx
        jnz bit_set
        mov result, 0
      bit_set:
        not cx
        and ax, cx
        mov [si], ax

        RELEASE_SPINLOCK(es:[bx])
    }
    return result;
}

#elif defined(__i386__) //&& defined(__FLAT__)

int __cdecl
__atomic_fetch_add_impl(volatile atomic_int *obj, const int value)
{
    int result = 0;
    __asm {
        mov esi, obj
        mov ecx, value
        mov ebx, OFFSET AtomicLockState
        
        ACQUIRE_SPINLOCK([ebx])

        mov eax, [esi]
        add ecx, eax
        mov [esi], ecx
        mov result, eax

        RELEASE_SPINLOCK([ebx])
    }
    return result;
}

signed char __cdecl
__atomic_compare_exchange_impl(volatile atomic_int *obj, int *expected, const int desired)
{
    signed char result = 1;
    __asm {
        mov esi, expected
        mov ecx, desired
        mov ebx, OFFSET AtomicLockState

        ACQUIRE_SPINLOCK([ebx])

        mov edi, obj
        mov eax, [esi]
        mov edx, [edi]
        cmp edx, eax
        jne not_equal
        mov [edi], ecx
        jmp done
      not_equal:
        mov [esi], edx
        mov result, 0
      done:
        RELEASE_SPINLOCK([ebx])
    }
    return result;
}

void __cdecl
__atomic_store_impl(volatile atomic_int *obj, const int value)
{
    __asm {
        mov esi, obj
        mov ecx, value
        mov ebx, OFFSET AtomicLockState

        ACQUIRE_SPINLOCK([ebx])

        mov [esi], ecx

        RELEASE_SPINLOCK([ebx])
    }
}

int __cdecl
__atomic_load_impl(const volatile atomic_int *obj)
{
    int result = 0;
    __asm {
        mov esi, obj
        mov ebx, OFFSET AtomicLockState

        ACQUIRE_SPINLOCK([ebx])

        mov eax, [esi]
        mov result, eax

        RELEASE_SPINLOCK([ebx])
    }
    return result;
}

int __cdecl
__atomic_exchange_impl(volatile atomic_int *obj, const int value)
{
    int result = 0;
    __asm {
        mov esi, obj
        mov ecx, value
        mov ebx, OFFSET AtomicLockState

        ACQUIRE_SPINLOCK([ebx])

        mov eax, [esi]
        mov [esi], ecx
        mov result, eax

        RELEASE_SPINLOCK([ebx])
    }
    return result;
}

int __cdecl
__atomic_add_impl(volatile atomic_int *obj, const int value)
{
    int result = 0;
    __asm {
        mov esi, obj
        mov ecx, value
        mov ebx, OFFSET AtomicLockState

        ACQUIRE_SPINLOCK([ebx])

        mov eax, [esi]
        add eax, ecx
        mov [esi], eax
        mov result, eax

        RELEASE_SPINLOCK([ebx])
    }
    return result;
}

int __cdecl
__atomic_inc_impl(volatile atomic_int *obj)
{
    int result = 0;
    __asm {
        mov esi, obj
        mov ebx, OFFSET AtomicLockState

        ACQUIRE_SPINLOCK([ebx])

        mov eax, [esi]
        inc eax
        mov [esi], eax
        mov result, eax

        RELEASE_SPINLOCK([ebx])
    }
    return result;
}

int __cdecl
__atomic_dec_impl(volatile atomic_int *obj)
{
    int result = 0;
    __asm {
        mov esi, obj
        mov ebx, OFFSET AtomicLockState

        ACQUIRE_SPINLOCK([ebx])

        mov eax, [esi]
        dec eax
        mov [esi], eax
        mov result, eax

        RELEASE_SPINLOCK([ebx])
    }
    return result;
}

signed char __cdecl
__atomic_bit_test_and_set(volatile atomic_int *obj, unsigned num)
{
    signed char result = 0;
    __asm {
        mov esi, obj
        mov ecx, num
        mov ebx, OFFSET AtomicLockState

        ACQUIRE_SPINLOCK([ebx])

        bts dword ptr [esi], ecx
        setc al
        mov result, al

        RELEASE_SPINLOCK([ebx])
    }
    return result;
}

signed char __cdecl
__atomic_bit_test_and_clear(volatile atomic_int *obj, unsigned num)
{
    signed char result = 0;
    __asm {
        mov esi, obj
        mov ecx, num
        mov ebx, OFFSET AtomicLockState

        ACQUIRE_SPINLOCK([ebx])

        btr dword ptr [esi], ecx
        setc al
        mov result, al

        RELEASE_SPINLOCK([ebx])
    }
    return result;
}

#endif

#endif
