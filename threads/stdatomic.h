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

typedef void* atomic_ptr;

#if defined(_M_IX86)

#define _cpu_pause() __asm _emit 0xf3 __asm _emit 0x90

#else

#define _cpu_pause() (void)0

#endif

#if defined(__i386__)

#if defined(__GNUC__) || defined(__clang__)

#define __locked_exchange_impl(obj, value) \
    __extension__ ({ \
        __typeof__(value) __result; \
        __asm__ __volatile__( \
            "xchg %0, %1" \
            : "=r" (__result), "+m" (*(obj)) \
            : "0" (value) \
            : "memory"); \
        __result; \
    })
#define __locked_exchange_ptr_impl __locked_exchange_impl

#define __locked_store_impl(obj, value) \
    __extension__ ({ \
        __typeof__(value) __result; \
        __asm__ __volatile__( \
            "xchg %0, %1" \
            : "=r"(__result), "=m" (*(obj)) \
            : "0" (value) \
            : "memory"); \
    })
#define __locked_store_ptr_impl __locked_store_impl

#define __locked_add_impl(obj, value) \
    __extension__ ({ \
        int __le, __ge; \
        __asm__ __volatile__( \
            "lock; addl %3, %0" \
            : "+m" (*(obj)), "=@ccle" (__le), "=@ccge" (__ge) \
            : "ri" (value) \
            : "memory", "cc"); \
        __ge && __le ? 0 : __ge ? 1 : -1; \
    })

#define __locked_inc_impl(obj) \
    __extension__ ({ \
        int __le, __ge; \
        __asm__ __volatile__( \
            "lock; incl %0" \
            : "+m" (*(obj)), "=@ccle" (__le), "=@ccge" (__ge) \
            : \
            : "memory", "cc"); \
        __ge && __le ? 0 : __ge ? 1 : -1; \
    })

#define __locked_dec_impl(obj) \
    __extension__ ({ \
        int __le, __ge; \
        __asm__ __volatile__( \
            "lock; decl %0" \
            : "+m" (*(obj)), "=@ccle" (__le), "=@ccge" (__ge) \
            : \
            : "memory", "cc"); \
        __ge && __le ? 0 : __ge ? 1 : -1; \
    })

#define __locked_bit_test_and_set(obj, bitnum) \
    __extension__ ({ \
        int __result; \
        __asm__ __volatile__( \
            "lock; btsl %2, %0" \
            : "+m" (*(obj)), "=@ccc" (__result) \
            : "ri" (bitnum) \
            : "memory", "cc"); \
        __result; \
    })

#define __locked_bit_test_and_clear(obj, bitnum) \
    __extension__ ({ \
        int __result; \
        __asm__ __volatile__( \
            "lock; btcl %2, %0" \
            : "+m" (*(obj)), "=@ccc" (__result) \
            : "ri" (bitnum) \
            : "memory", "cc"); \
        __result; \
    })

#if _M_IX86 >= 400

#define __locked_compare_exchange_impl(obj, expected, desired) \
    __extension__ ({ \
        __typeof__(*(expected)) __tmp; \
        __typeof__(expected)   __pold = (expected); \
        __typeof__(*(expected))  __old = *__pold; \
        int __result; /* result in zero flag */ \
        __asm__ __volatile__( \
            "lock; cmpxchg %3, %2" \
            : "=a" (__tmp), "=@ccz" (__result), "+m" (*(obj)) \
            : "r" (desired), "0" (__old) \
            : "memory", "cc"); \
        if (!__result) *__pold = __tmp; \
        __result; \
    })
#define __locked_compare_exchange_ptr_impl __locked_compare_exchange_impl

#define __locked_fetch_add_impl(obj, value) \
    __extension__ ({ \
        __typeof__(value) __result; \
        __asm__ __volatile__( \
            "lock; xadd %0, %1" \
            : "=r" (__result), "+m" (*(obj)) \
            : "0" (value) \
            : "memory", "cc"); \
        __result; \
    })

#endif

#elif defined(__WATCOMC__)

extern int __locked_exchange_impl(volatile atomic_int *obj, const int value);
#pragma aux __locked_exchange_impl = \
    "xchg [edx], eax" \
    __value [eax]  \
    __parm [edx] [eax]

extern void* __locked_exchange_ptr_impl(volatile atomic_ptr *obj, const void *value);
#pragma aux __locked_exchange_ptr_impl = \
    "xchg [edx], eax" \
    __value [eax]  \
    __parm [edx] [eax]

extern void __locked_store_impl(volatile atomic_int *obj, const int value);
#pragma aux __locked_store_impl = \
    "xchg [edx], eax" \
    __parm [edx] [eax] \
    __modify [eax]

extern void __locked_store_ptr_impl(volatile atomic_ptr *obj, const void *value);
#pragma aux __locked_store_ptr_impl = \
    "xchg [edx], eax" \
    __parm [edx] [eax] \
    __modify [eax]

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

extern signed char __locked_compare_exchange_ptr_impl(volatile atomic_ptr *obj, void **expected, const void *desired);
#pragma aux __locked_compare_exchange_ptr_impl = \
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

#pragma warning( disable : 4035 )

static __inline int __cdecl
__locked_exchange_impl(volatile atomic_int *obj, const int desired)
{
    __asm {
        mov eax, desired
        mov ecx, obj
        xchg [ecx], eax
    }
}

static __inline void* __cdecl
__locked_exchange_ptr_impl(volatile atomic_ptr *obj, const void *desired)
{
    __asm {
        mov eax, desired
        mov ecx, obj
        xchg [ecx], eax
    }
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

static __inline void __cdecl
__locked_store_ptr_impl(volatile atomic_ptr *obj, const void *value)
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

static __inline signed char __cdecl
__locked_compare_exchange_ptr_impl(volatile atomic_ptr *obj, void **expected, const void *desired)
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

#pragma warning( default : 4035 )

#endif // _MSC_VER

#define __locked_load_impl(obj)  (((volatile atomic_int *)(obj))->_value)
#define __locked_load_ptr_impl(obj)  (*(volatile atomic_ptr *)(obj))

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

#define __locked_load_impl(obj)  (((volatile atomic_int *)(obj))->_value)

#if _M_IX86 >= 300

extern void* __locked_exchange_ptr_impl(volatile atomic_ptr *obj, const void *value);
#pragma aux __locked_exchange_ptr_impl = \
    "shl edx, 16" \
    "movzx eax, ax" \
    "or eax, edx" \
    "xchg es:[bx], eax" \
    "mov edx, eax" \
    "shr edx, 16" \
    __value [dx ax]  \
    __parm [es bx] [dx ax]

extern void __locked_store_ptr_impl(volatile atomic_ptr *obj, const void *value);
#pragma aux __locked_store_ptr_impl = \
    "shl edx, 16" \
    "movzx eax, ax" \
    "or eax, edx" \
    "xchg es:[bx], eax" \
    __parm [es bx] [dx ax] \
    __modify [ax]

extern void* __locked_load_ptr_impl(volatile atomic_ptr *obj);
#pragma aux __locked_load_ptr_impl = \
    "mov eax, es:[bx]" \
    "mov edx, eax" \
    "shr edx, 16" \
    __value [dx ax]  \
    __parm [es bx]

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

extern signed char __locked_compare_exchange_ptr_impl(volatile atomic_ptr *obj, void **expected, const void *desired);
#pragma aux __locked_compare_exchange_ptr_impl = \
    "mov eax, dword ptr [si]" \
    "shl ecx, 16" \
    "movzx edx, dx" \
    "or ecx, edx" \
    "lock cmpxchg dword ptr es:[bx], ecx" \
    "je succ" \
    "mov dword ptr [si], eax" \
    "succ: setz al" \
    __value [al]  \
    __parm [es bx] [ds si] [cx dx]

#endif // _M_IX86 >= 400

#elif defined(_MSC_VER)
#error Define 16-bit atomics for MSVC
#endif

#elif defined(_MSC_VER) && defined(_M_MRX000) && _M_MRX000 >= 4000

#pragma warning( disable : 4035 )

static __inline int __cdecl __locked_exchange_impl(volatile atomic_int *obj, const int value)
{
	__asm(
		"1: move %t1, %1;"
		"ll %v0, 0(%0);"
		"sc %t1, 0(%0);"
		"beq %t1, 0, 1b", obj, value);
}

static __inline void* __cdecl __locked_exchange_ptr_impl(volatile atomic_ptr *obj, const void *value)
{
	__asm(
		"1: move %t1, %1;"
		"ll %v0, 0(%0);"
		"sc %t1, 0(%0);"
		"beq %t1, 0, 1b", obj, value);
}


static __inline int __cdecl __locked_load_impl(volatile atomic_int *obj)
{
	__asm(
		"sync;"
		"lw %v0, 0(%0);"
		"sync"
		, obj);
}

static __inline void* __cdecl __locked_load_ptr_impl(volatile atomic_ptr *obj)
{
	__asm(
		"sync;"
		"lw %v0, 0(%0);"
		"sync"
		, obj);
}

static __inline void __cdecl __locked_store_impl(volatile atomic_int *obj, const int value)
{
	__asm(
		"sync;"
		"sw %1, 0(%0);"
		"sync"
		, obj, value);
}

static __inline void __cdecl __locked_store_ptr_impl(volatile atomic_ptr *obj, const void *value)
{
	__asm(
		"sync;"
		"sw %1, 0(%0);"
		"sync"
		, obj, value);
}

static __inline int __cdecl __locked_add_impl(volatile atomic_int *obj, const int value)
{
	__asm(
		"1: ll %t0, 0(%0);"
		"addu %t1, %t0, %1;"
		"move %v0, %t1;"
		"sc %t1, 0(%0);"
		"beq %t1, 0, 1b", obj, value);
}

static __inline int __cdecl __locked_fetch_add_impl(volatile atomic_int *obj, const int value)
{
	__asm(
		"1: ll %v0, 0(%0);"
		"addu %t1, %v0, %1;"
		"sc %t1, 0(%0);"
		"beq %t1, 0, 1b", obj, value);
}

static __inline int __cdecl __locked_inc_impl(volatile atomic_int *obj)
{
	__asm(
		"1: ll %t0, 0(%0);"
		"addiu %t1, %t0, 1;"
		"move %v0, %t1;"
		"sc %t1, 0(%0);"
		"beq %t1, 0, 1b", obj);
}

static __inline int __cdecl __locked_dec_impl(volatile atomic_int *obj)
{
	__asm(
		"1: ll %t0, 0(%0);"
		"addiu %t1, %t0, -1;"
		"move %v0, %t1;"
		"sc %t1, 0(%0);"
		"beq %t1, 0, 1b", obj);
}

static __inline int __cdecl __locked_compare_exchange_strong_impl(volatile atomic_int *obj, int *expected, const int desired)
{
	__asm(
		"lw %t2, 0(%1);"			//load expected
		"move %v0, %zero;"			//assume fail
		"1: move %t1, %2;"
		"ll %t0, 0(%0);"
		"bne %t0, %t2, 2f;"
		"sc %t1, 0(%0);"
		"beq %t1, 0, 1b;"			//can't fail spuriously
		"li %v0, 1;"
		"j 3f;"
		"2: sw %t0, 0(%1);"
		"3: ", obj, expected, desired);
}

static __inline void* __cdecl __locked_compare_exchange_ptr_strong_impl(volatile atomic_ptr *obj, void **expected, const void *desired)
{
	__asm(
		"lw %t2, 0(%1);"			//load expected
		"move %v0, %zero;"			//assume fail
		"1: move %t1, %2;"
		"ll %t0, 0(%0);"
		"bne %t0, %t2, 2f;"
		"sc %t1, 0(%0);"
		"beq %t1, 0, 1b;"			//can't fail spuriously
		"li %v0, 1;"
		"j 3f;"
		"2: sw %t0, 0(%1);"
		"3: ", obj, expected, desired);
}

static __inline int __cdecl __locked_compare_exchange_weak_impl(volatile atomic_int *obj, int *expected, const int desired)
{
	__asm(
		"lw %t2, 0(%1);"			//load expected
		"move %v0, %zero;"			//assume fail
		"ll %t0, 0(%0);"
		"bne %t0, %t2, 1f;"
		"sc %2, 0(%0);"
		"beq %2, 0, 1f;"			//can fail spuriously
		"li %v0, 1;"
		"j 2f;"
		"1: sw %t0, 0(%1);"
		"2: ", obj, expected, desired);
}

static __inline void* __cdecl __locked_compare_exchange_ptr_weak_impl(volatile atomic_ptr *obj, void **expected, const void *desired)
{
	__asm(
		"lw %t2, 0(%1);"			//load expected
		"move %v0, %zero;"			//assume fail
		"ll %t0, 0(%0);"
		"bne %t0, %t2, 1f;"
		"sc %2, 0(%0);"
		"beq %2, 0, 1f;"			//can fail spuriously
		"li %v0, 1;"
		"j 2f;"
		"1: sw %t0, 0(%1);"
		"2: ", obj, expected, desired);

}
#pragma warning( default : 4035 )

#endif // _M_MRX000

#if defined(_M_IX86) && _M_IX86 < 400

int __cdecl
__atomic_fetch_add_impl(volatile atomic_int *obj, const int value);

signed char __cdecl
__atomic_compare_exchange_impl(volatile atomic_int *obj, int *expected, const int desired);

signed char __cdecl
__atomic_compare_exchange_ptr_impl(volatile atomic_ptr *obj, void **expected, const void *desired);

void __cdecl
__atomic_store_impl(volatile atomic_int *obj, const int value);

void __cdecl
__atomic_store_ptr_impl(volatile atomic_ptr *obj, const void *value);

int __cdecl
__atomic_load_impl(volatile atomic_int *obj);

void *__cdecl
__atomic_load_ptr_impl(volatile atomic_ptr *obj);

int __cdecl
__atomic_exchange_impl(volatile atomic_int *obj, const int value);

void* __cdecl
__atomic_exchange_ptr_impl(volatile atomic_ptr *obj, const void *value);

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
#define atomic_load_ptr __atomic_load_ptr_impl
#define atomic_load_explicit(obj,order) __atomic_load_impl(obj)

#define atomic_store __atomic_store_impl
#define atomic_store_ptr __atomic_store_ptr_impl
#define atomic_store_explicit(obj,value,order) __atomic_store_impl((obj), (value))

#define atomic_fetch_add __atomic_fetch_add_impl
#define atomic_fetch_add_explicit(obj,value,order) __atomic_fetch_add_impl((obj), (value))

#define atomic_exchange __atomic_exchange_impl
#define atomic_exchange_ptr(obj, value) __atomic_exchange_ptr_impl((volatile atomic_ptr*)(obj), value)
#define atomic_exchange_explicit(obj,desired,order) __atomic_exchange_impl((obj), (desired))

#define atomic_compare_exchange_strong __atomic_compare_exchange_impl
#define atomic_compare_exchange_weak __atomic_compare_exchange_impl
#define atomic_compare_exchange_ptr_strong(obj, expected, desired) __atomic_compare_exchange_ptr_impl(obj, (void**)(expected), desired)
#define atomic_compare_exchange_ptr_weak(obj, expected, desired) __atomic_compare_exchange_ptr_impl(obj, (void**)(expected), desired)

#define _atomic_bit_test_and_set __atomic_bit_test_and_set
#define _atomic_bit_test_and_clear __atomic_bit_test_and_clear

#define _atomic_add __atomic_add_impl
#define _atomic_inc __atomic_inc_impl
#define _atomic_dec __atomic_dec_impl

#else

#define atomic_load __locked_load_impl
#define atomic_load_ptr __locked_load_ptr_impl
#define atomic_load_explicit(obj,order) __locked_load_impl(obj)

#define atomic_store __locked_store_impl
#define atomic_store_ptr __locked_store_ptr_impl
#define atomic_store_explicit(obj,value,order) __locked_store_impl((obj), (value))

#define atomic_fetch_add __locked_fetch_add_impl
#define atomic_fetch_add_explicit(obj,value,order) __locked_fetch_add_impl((obj), (value))

#define atomic_exchange __locked_exchange_impl
#define atomic_exchange_ptr __locked_exchange_ptr_impl
#define atomic_exchange_explicit(obj,desired,order) __locked_exchange_impl((obj), (desired))

#if defined(_M_IX86)

#define atomic_compare_exchange_strong __locked_compare_exchange_impl
#define atomic_compare_exchange_weak __locked_compare_exchange_impl
#define atomic_compare_exchange_ptr_strong(obj, expected, desired) __locked_compare_exchange_ptr_impl((volatile atomic_ptr*)(obj), (void**)(expected), desired)
#define atomic_compare_exchange_ptr_weak(obj, expected, desired) __locked_compare_exchange_ptr_impl((volatile atomic_ptr*)(obj), (void**)(expected), desired)

#define _atomic_bit_test_and_set __locked_bit_test_and_set
#define _atomic_bit_test_and_clear __locked_bit_test_and_clear

#elif defined(_M_MRX000) && _M_MRX000 >= 4000

#define atomic_compare_exchange_strong __locked_compare_exchange_strong_impl
#define atomic_compare_exchange_weak __locked_compare_exchange_weak_impl
#define atomic_compare_exchange_ptr_strong(obj, expected, desired) __locked_compare_exchange_ptr_strong_impl((volatile atomic_ptr*)(obj), (void**)(expected), desired)
#define atomic_compare_exchange_ptr_weak(obj, expected, desired) __locked_compare_exchange_ptr_weak_impl((volatile atomic_ptr*)(obj), (void**)(expected), desired)

#endif

#define _atomic_add __locked_add_impl
#define _atomic_inc __locked_inc_impl
#define _atomic_dec __locked_dec_impl

#endif

#define _locked_load __locked_load_impl
#define _locked_load_ptr(obj) __locked_load_ptr_impl((volatile atomic_ptr*)(obj))
#define _locked_load_explicit(obj,order) __locked_load_impl(obj)

#define _locked_store __locked_store_impl
#define _locked_store_ptr(obj, value) __locked_store_ptr_impl((volatile atomic_ptr*)(obj), value)
#define _locked_store_explicit(obj,value,order) __locked_store_impl((obj), (value))

#define _locked_exchange __locked_exchange_impl
#define _locked_exchange_ptr(obj, value) __locked_exchange_ptr_impl((volatile atomic_ptr*)(obj), value)
#define _locked_exchange_explicit(obj,desired,order) __locked_exchange_impl((obj), (desired))

#if (defined(_M_IX86) && _M_IX86 >= 300) || (defined(_M_MRX000) && _M_MRX000 >= 4000)

#define _locked_add __locked_add_impl
#define _locked_inc __locked_inc_impl
#define _locked_dec __locked_dec_impl
#define _locked_bit_test_and_set __locked_bit_test_and_set
#define _locked_bit_test_and_clear __locked_bit_test_and_clear

#endif

#if (defined(_M_IX86) && _M_IX86 >= 400) || (defined(_M_MRX000) && _M_MRX000 >= 4000)

#define _locked_fetch_add __locked_fetch_add_impl
#define _locked_fetch_add_explicit(obj,value,order) __locked_fetch_add_impl((obj), (value))

#if (defined(_M_IX86) && _M_IX86 >= 400)

#define _locked_compare_exchange_strong __locked_compare_exchange_impl
#define _locked_compare_exchange_weak __locked_compare_exchange_impl
#define _locked_compare_exchange_ptr_strong(obj, expected, desired) __locked_compare_exchange_ptr_impl((volatile atomic_ptr*)(obj), (void**)(expected), desired)
#define _locked_compare_exchange_ptr_weak(obj, expected, desired) __locked_compare_exchange_ptr_impl((volatile atomic_ptr*)(obj), (void**)(expected), desired)

#elif defined(_M_MRX000) && _M_MRX000 >= 4000

#define _locked_compare_exchange_strong __locked_compare_exchange_strong_impl
#define _locked_compare_exchange_weak __locked_compare_exchange_weak_impl
#define _locked_compare_exchange_ptr_strong(obj, expected, desired) __locked_compare_exchange_ptr_strong_impl((volatile atomic_ptr*)(obj), (void**)(expected), desired)
#define _locked_compare_exchange_ptr_weak(obj, expected, desired) __locked_compare_exchange_ptr_weak_impl((volatile atomic_ptr*)(obj), (void**)(expected), desired)

#endif

#endif
#endif
