/*
* Basic C11-style atomics library
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

#ifndef STDATOMIC_H
#define STDATOMIC_H

#if (defined(M_I86) || defined(_M_I86)) && !defined(__I86__)
#define __I86__ 1
#endif
#if (defined(i386) || defined(__i386) || defined(_M_IX86)) && !defined(__i386__) && !defined(__I86__)
#define __i386__ 1
#endif

typedef enum memory_order
{
    memory_order_relaxed,
    memory_order_consume,
    memory_order_acquire,
    memory_order_release,
    memory_order_acq_rel,
    memory_order_seq_cst
} memory_order;

typedef struct atomic_int {
    int     _value;
} atomic_int;

#if defined(__WATCOMC__)

extern void _cpu_pause(void);
#pragma aux _cpu_pause = \
    "db 0xf3, 0x90"

#elif defined(_MSC_VER)

static __inline void _cpu_pause(void)
{
    __asm {
        _emit 0xf3
        _emit 0x90
    }
}

#endif
#if defined(__i386__)

#if defined(__WATCOMC__)

extern int __locked_exchange_impl(volatile atomic_int *obj, const int value);
#pragma aux __locked_exchange_impl = \
    "xchg [edx], eax" \
    __value [eax]  \
    __parm [edx] [eax]

extern void __locked_store_impl(volatile atomic_int *obj, const int value);
#pragma aux __locked_store_impl = \
    "xchg [edx], eax" \
    __parm [edx] [eax] \
    __modify [eax]

extern int __locked_load_impl(const volatile atomic_int *obj);
#pragma aux __locked_load_impl = \
    "mov eax, [edx]" \
    __value [eax]  \
    __parm [edx]

extern int __locked_add_impl(volatile atomic_int *obj, const int value);
#pragma aux __locked_add_impl = \
    "lock add [edx], eax" \
    "setg al" \
    "setl cl" \
    "sub al, cl" \
    "movsx eax, al" \
    __value [eax] \
    __modify [cl] \
    __parm [edx] [eax]

extern int __locked_inc_impl(volatile atomic_int *obj);
#pragma aux __locked_inc_impl = \
    "lock inc dword ptr [edx]" \
    "setg al" \
    "setl cl" \
    "sub al, cl" \
    "movsx eax, al" \
    __value [eax] \
    __modify [cl] \
    __parm [edx]

extern int __locked_dec_impl(volatile atomic_int *obj);
#pragma aux __locked_dec_impl = \
    "lock dec dword ptr [edx]" \
    "setg al" \
    "setl cl" \
    "sub al, cl" \
    "movsx eax, al" \
    __value [eax] \
    __modify [cl] \
    __parm [edx]

extern signed char __locked_bit_test_and_set(volatile atomic_int *obj, unsigned num);
#pragma aux __locked_bit_test_and_set = \
    "lock bts dword ptr [edx], eax" \
    "setc al" \
    __value [al]  \
    __parm [edx] [eax]

extern signed char __locked_bit_test_and_clear(volatile atomic_int *obj, unsigned num);
#pragma aux __locked_bit_test_and_clear = \
    "lock btr dword ptr [edx], eax" \
    "setc al" \
    __value [al]  \
    __parm [edx] [eax]

#if _M_IX86 >= 400

extern signed char __locked_compare_exchange_impl(volatile atomic_int *obj, int *expected, const int desired);
#pragma aux __locked_compare_exchange_impl = \
    "mov eax, [ebx]" \
    "lock cmpxchg [edx], ecx" \
    "je succ" \
    "mov [ebx], eax" \
    "succ: setz al" \
    __value [al]  \
    __parm [edx] [ebx] [ecx]

extern int __locked_fetch_add_impl(volatile atomic_int *obj, const int value);
#pragma aux __locked_fetch_add_impl = \
    "lock xadd [edx], eax" \
    __value [eax]  \
    __parm [edx] [eax]

#endif // _M_IX86 >= 400

#elif defined(_MSC_VER)

static __inline int __cdecl
__locked_exchange_impl(volatile atomic_int *obj, const int desired)
{
    __asm {
        mov eax, desired
        mov ecx, obj
        xchg [ecx], eax
    }
}

static __inline int __cdecl
__locked_load_impl(volatile atomic_int *obj)
{
    return obj->_value;
    /*__asm {
        mov ecx, obj
        mov eax, dword ptr [ecx]
    }*/
}

static __inline void __cdecl
__locked_store_impl(volatile atomic_int *obj, const int value)
{
    __asm {
        mov edx, value
        mov ecx, obj
        xchg [ecx], edx
    }
}

static __inline int __cdecl
__locked_add_impl(volatile atomic_int *obj, const int value)
{
    _asm {
        mov ecx, obj
        mov eax, value
        lock add [ecx], eax
        setg al
        setl cl
        sub  al, cl
        movsx eax, al
    }
}

static __inline int __cdecl
__locked_inc_impl(volatile atomic_int *obj)
{
    _asm {
        mov ecx, obj
        lock inc dword ptr [ecx]
        setg al
        setl cl
        sub  al, cl
        movsx eax, al
    }
}

static __inline int __cdecl
__locked_dec_impl(volatile atomic_int *obj)
{
    _asm {
        mov ecx, obj
        lock dec dword ptr [ecx]
        setg al
        setl cl
        sub  al, cl
        movsx eax, al
    }
}

static __inline signed char __cdecl 
__locked_bit_test_and_set(volatile atomic_int *obj, unsigned num)
{
    _asm {
        mov ecx, obj
        mov eax, num
        lock bts dword ptr [ecx], eax
        setc al
    }
}

static __inline signed char __cdecl 
__locked_bit_test_and_clear(volatile atomic_int *obj, unsigned num)
{
    _asm {
        mov ecx, obj
        mov eax, num
        lock btr dword ptr [ecx], eax
        setc al
    }
}

#if _M_IX86 >= 400

static __inline signed char __cdecl
__locked_compare_exchange_impl(volatile atomic_int *obj, int *expected, const int desired)
{
    __asm {
        mov ebx, desired
        mov edx, expected
        mov ecx, obj
        mov eax, [edx]
        lock cmpxchg [ecx], ebx
        je succ
        mov [edx], eax
      succ:
        setz al
    }
}

static __inline int __cdecl
__locked_fetch_add_impl(volatile atomic_int *obj, const int value)
{
    __asm {
        mov eax, value
        mov ecx, obj
        lock xadd [ecx], eax
    }
}

#endif // _M_IX86 >= 400

#endif // _MSC_VER

#elif defined(__I86__)

#if defined(__WATCOMC__)

extern int __locked_exchange_impl(volatile atomic_int *obj, const int value);
#pragma aux __locked_exchange_impl = \
    "xchg es:[bx], ax" \
    __value [ax]  \
    __parm [es bx] [ax]

extern void __locked_store_impl(volatile atomic_int *obj, const int value);
#pragma aux __locked_store_impl = \
    "xchg es:[bx], ax" \
    __parm [es bx] [ax] \
    __modify [ax]

extern int __locked_load_impl(const volatile atomic_int *obj);
#pragma aux __locked_load_impl = \
    "mov ax, es:[bx]" \
    __value [ax]  \
    __parm [es bx]

#if _M_IX86 >= 300

extern int __locked_add_impl(volatile atomic_int *obj, const int value);
#pragma aux __locked_add_impl = \
    "lock add es:[bx], ax" \
    "setg al" \
    "setl cl" \
    "sub al, cl" \
    "cbw" \
    __value [ax] \
    __modify [cl] \
    __parm [es bx] [ax]

extern int __locked_inc_impl(volatile atomic_int *obj);
#pragma aux __locked_inc_impl = \
    "lock inc word ptr es:[bx]" \
    "setg al" \
    "setl cl" \
    "sub al, cl" \
    "cbw" \
    __value [ax] \
    __modify [cl] \
    __parm [es bx]

extern int __locked_dec_impl(volatile atomic_int *obj);
#pragma aux __locked_dec_impl = \
    "lock dec word ptr es:[bx]" \
    "setg al" \
    "setl cl" \
    "sub al, cl" \
    "cbw" \
    __value [ax] \
    __modify [cl] \
    __parm [es bx]

extern signed char __locked_bit_test_and_set(volatile atomic_int *obj, unsigned num);
#pragma aux __locked_bit_test_and_set = \
    "lock bts word ptr es:[bx], ax" \
    "setc al" \
    __value [al]  \
    __parm [es bx] [ax]

extern signed char __locked_bit_test_and_clear(volatile atomic_int *obj, unsigned num);
#pragma aux __locked_bit_test_and_clear = \
    "lock btr word ptr es:[bx], ax" \
    "setc al" \
    __value [al]  \
    __parm [es bx] [ax]

#endif

#if _M_IX86 >= 400

extern int __locked_fetch_add_impl(volatile atomic_int *obj, const int value);
#pragma aux __locked_fetch_add_impl = \
    "lock xadd es:[bx], ax" \
    __value [ax]  \
    __parm [es bx] [ax]

extern signed char __locked_compare_exchange_impl(volatile atomic_int *obj, int *expected, const int desired);
#pragma aux __locked_compare_exchange_impl = \
    "mov ax, [si]" \
    "lock cmpxchg es:[bx], cx" \
    "je succ" \
    "mov [si], ax" \
    "succ: setz al" \
    __value [al]  \
    __parm [es bx] [ds si] [cx]

#endif // _M_IX86 >= 400

#elif defined(_MSC_VER)
#error Define 16-bit atomics for MSVC
#endif

#endif // __i86__

#if _M_IX86 < 400

int __cdecl
__atomic_fetch_add_impl(volatile atomic_int *obj, const int value);

signed char __cdecl
__atomic_compare_exchange_impl(volatile atomic_int *obj, int *expected, const int desired);

void __cdecl
__atomic_store_impl(volatile atomic_int *obj, const int value);

int __cdecl
__atomic_load_impl(const volatile atomic_int *obj);

int __cdecl
__atomic_exchange_impl(volatile atomic_int *obj, const int value);

int __cdecl
__atomic_add_impl(volatile atomic_int *obj, const int value);

int __cdecl
__atomic_inc_impl(volatile atomic_int *obj);

int __cdecl
__atomic_dec_impl(volatile atomic_int *obj);

signed char __cdecl
__atomic_bit_test_and_set(volatile atomic_int *obj, unsigned num);

signed char __cdecl
__atomic_bit_test_and_clear(volatile atomic_int *obj, unsigned num);

#define atomic_load __atomic_load_impl
#define atomic_load_explicit(obj,order) __atomic_load_impl(obj)

#define atomic_store __atomic_store_impl
#define atomic_store_explicit(obj,value,order) __atomic_store_impl((obj), (value))

#define atomic_fetch_add __atomic_fetch_add_impl
#define atomic_fetch_add_explicit(obj,value,order) __atomic_fetch_add_impl((obj), (value))

#define atomic_exchange __atomic_exchange_impl
#define atomic_exchange_explicit(obj,desired,order) __atomic_exchange_impl((obj), (desired))

#define atomic_compare_exchange_strong __atomic_compare_exchange_impl
#define atomic_compare_exchange_weak __atomic_compare_exchange_impl

#define _atomic_bit_test_and_set __atomic_bit_test_and_set
#define _atomic_bit_test_and_clear __atomic_bit_test_and_clear

#define _atomic_add __atomic_add_impl
#define _atomic_inc __atomic_inc_impl
#define _atomic_dec __atomic_dec_impl

#else

#define atomic_load __locked_load_impl
#define atomic_load_explicit(obj,order) __locked_load_impl(obj)

#define atomic_store __locked_store_impl
#define atomic_store_explicit(obj,value,order) __locked_store_impl((obj), (value))

#define atomic_fetch_add __locked_fetch_add_impl
#define atomic_fetch_add_explicit(obj,value,order) __locked_fetch_add_impl((obj), (value))

#define atomic_exchange __locked_exchange_impl
#define atomic_exchange_explicit(obj,desired,order) __locked_exchange_impl((obj), (desired))

#define atomic_compare_exchange_strong __locked_compare_exchange_impl
#define atomic_compare_exchange_weak __locked_compare_exchange_impl

#define _atomic_add __locked_add_impl
#define _atomic_inc __locked_inc_impl
#define _atomic_dec __locked_dec_impl

#define _atomic_bit_test_and_set __locked_bit_test_and_set
#define _atomic_bit_test_and_clear __locked_bit_test_and_clear

#endif

#define _locked_load __locked_load_impl
#define _locked_load_explicit(obj,order) __locked_load_impl(obj)

#define _locked_store __locked_store_impl
#define _locked_store_explicit(obj,value,order) __locked_store_impl((obj), (value))

#define _locked_exchange __locked_exchange_impl
#define _locked_exchange_explicit(obj,desired,order) __locked_exchange_impl((obj), (desired))

#if _M_IX86 >= 300

#define _locked_add __locked_add_impl
#define _locked_inc __locked_inc_impl
#define _locked_dec __locked_dec_impl
#define _locked_bit_test_and_set __locked_bit_test_and_set
#define _locked_bit_test_and_clear __locked_bit_test_and_clear

#endif

#if _M_IX86 >= 400

#define _locked_fetch_add __locked_fetch_add_impl
#define _locked_fetch_add_explicit(obj,value,order) __locked_fetch_add_impl((obj), (value))

#define _locked_compare_exchange_strong __locked_compare_exchange_impl
#define _locked_compare_exchange_weak __locked_compare_exchange_impl

#endif
#endif
