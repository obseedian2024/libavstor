# libavstor - library to create files with hierarchial data
libavstor is a highly experimental, portable low-level C library to create files with hierarchial data similar to the Windows registry. Keys and values are stored in the file using AVL trees. The library is implemented in a single source file and a header file, allowing easy integration into any project.

## Features
* Either 64-bit or 32-bit files, allowing 16 TB or 2 GB maximum file sizes, respectively
* Run-time choice of external IO routines, or default internal IO implementation
* Flexible data types for keys via user-defined key comparer functions
* Data types for values: int32, int64, double, short binary/character (240 bytes or less)
* Manual or auto-commit option
* Setting maximum cache size
* Special link value type to create pointers to arbitrary nodes
* Optionally thread-safe (supported on certain platforms/compilers only)
* Cross-platform: Windows (from NT 3.51 up), UNIX, FreeBSD, Linux, DOS (real or protected mode), OS/2 (16 or 32 bit)

## Compiling libavstor
libavstor is written in C89 dialect, so most recent and even not so recent compilers should work. There is no requirement for a particular build system.
* clang: Tested on Windows and FreeBSD, either 32-bit or 64-bit
* Visual Studio: All versions should work from 4.1 up (should even work on NT 3.51 MIPS)
* OpenWatcom 1.9: Windows NT 3.51+, DOS, DOS 32-bit (using DOS extender), OS/2 1.3+ (16 or 32 bit)
* GCC: tested under WSL

### Build options
Several features can be included/enabled via macro definitions:
* `AVSTOR_CONFIG_THREAD_SAFE`: Enable thread-safe operation. Locking is implemented on UNIX/Linux using C11 `threads.h` and `stdatomic.h` when using clang or other C11 compatible compilers. On Windows, SRW locks and condition variables are used for newer (Vista+) versions. Older versions will have custom partial `threads.h` and `stdatomic.h` implementation to support OpenWatcom or older MSVC versions.
* `AVSTOR_CONFIG_FILE_64BIT`: Use 64-bit internal pointers. This allows files up to 16 TB (for now) however it will increase file size compared to 32-bit pointers.
* `AVSTOR_CONFIG_FORCE_C11_THREADS`: Force the use of C11 mutexes and condition variables. Required for older compilers with no native `threads.h` and `stdatomic.h` implementation. 
--------
libavstor and this documentation is a work-in-progress, see source for API and examples.
