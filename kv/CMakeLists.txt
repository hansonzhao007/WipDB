# Setup the minimum version required of CMake to generate the Makefile
cmake_minimum_required(VERSION 3.9)
project (KV VERSION 0.0.1 LANGUAGES C CXX)


set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()


include(CheckCCompilerFlag)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "ppc64le")
  CHECK_C_COMPILER_FLAG("-maltivec" HAS_ALTIVEC)
  if(HAS_ALTIVEC)
    message(STATUS " HAS_ALTIVEC yes")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -maltivec")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -maltivec")
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -mcpu=power8")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mcpu=power8")
  endif(HAS_ALTIVEC)
endif(CMAKE_SYSTEM_PROCESSOR MATCHES "ppc64le")

option(PORTABLE "build a portable binary" OFF)
option(FORCE_SSE42 "force building with SSE4.2, even when PORTABLE=ON" OFF)
if(PORTABLE)
  # MSVC does not need a separate compiler flag to enable SSE4.2; if nmmintrin.h
  # is available, it is available by default.
  if(FORCE_SSE42 AND NOT MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -msse4.2 -mpclmul")
  endif()
else()
  if(MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /arch:AVX2")
  else()
    if(NOT HAVE_POWER8)
      set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native")
    endif()
  endif()
endif()

include(CheckCXXSourceCompiles)
if(NOT MSVC)
  set(CMAKE_REQUIRED_FLAGS "-msse4.2 -mpclmul")
endif()
CHECK_CXX_SOURCE_COMPILES("
#include <cstdint>
#include <nmmintrin.h>
#include <wmmintrin.h>
int main() {
  volatile uint32_t x = _mm_crc32_u32(0, 0);
  const auto a = _mm_set_epi64x(0, 0);
  const auto b = _mm_set_epi64x(0, 0);
  const auto c = _mm_clmulepi64_si128(a, b, 0x00);
  auto d = _mm_cvtsi128_si64(c);
}
" HAVE_SSE42)
unset(CMAKE_REQUIRED_FLAGS)
if(HAVE_SSE42)
  add_definitions(-DHAVE_SSE42)
  add_definitions(-DHAVE_PCLMUL)
elseif(FORCE_SSE42)
  message(FATAL_ERROR "FORCE_SSE42=ON but unable to compile with SSE4.2 enabled")
endif()

CHECK_CXX_SOURCE_COMPILES("
#if defined(_MSC_VER) && !defined(__thread)
#define __thread __declspec(thread)
#endif
int main() {
  static __thread int tls;
}
" HAVE_THREAD_LOCAL)
if(HAVE_THREAD_LOCAL)
  add_definitions(-DROCKSDB_SUPPORT_THREAD_LOCAL)
endif()

option(FAIL_ON_WARNINGS "Treat compile warnings as errors" OFF)
if(FAIL_ON_WARNINGS)
  if(MSVC)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} /WX")
  else() # assume GCC
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Werror")
  endif()
endif()


if(CMAKE_COMPILER_IS_GNUCXX)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-builtin-memcmp")
endif()

if(CMAKE_SYSTEM_NAME MATCHES "Cygwin")
  add_definitions(-fno-builtin-memcmp -DCYGWIN)
elseif(CMAKE_SYSTEM_NAME MATCHES "Darwin")
  add_definitions(-DOS_MACOSX)
  if(CMAKE_SYSTEM_PROCESSOR MATCHES arm)
    add_definitions(-DIOS_CROSS_COMPILE -DROCKSDB_LITE)
    # no debug info for IOS, that will make our library big
    add_definitions(-DNDEBUG)
  endif()
elseif(CMAKE_SYSTEM_NAME MATCHES "Linux")
  add_definitions(-DOS_LINUX)
elseif(CMAKE_SYSTEM_NAME MATCHES "SunOS")
  add_definitions(-DOS_SOLARIS)
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
  add_definitions(-DOS_FREEBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "NetBSD")
  add_definitions(-DOS_NETBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "OpenBSD")
  add_definitions(-DOS_OPENBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "DragonFly")
  add_definitions(-DOS_DRAGONFLYBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "Android")
  add_definitions(-DOS_ANDROID)
elseif(CMAKE_SYSTEM_NAME MATCHES "Windows")
  add_definitions(-DWIN32 -DOS_WIN -D_MBCS -DWIN64 -DNOMINMAX)
  if(MINGW)
    add_definitions(-D_WIN32_WINNT=_WIN32_WINNT_VISTA)
  endif()
endif()

if(NOT WIN32)
  add_definitions(-DROCKSDB_PLATFORM_POSIX -DROCKSDB_LIB_IO_POSIX)
endif()

option(WITH_FALLOCATE "build with fallocate" ON)
if(WITH_FALLOCATE)
  CHECK_CXX_SOURCE_COMPILES("
#include <fcntl.h>
#include <linux/falloc.h>
int main() {
 int fd = open(\"/dev/null\", 0);
 fallocate(fd, FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE, 0, 1024);
}
" HAVE_FALLOCATE)
  if(HAVE_FALLOCATE)
    add_definitions(-DROCKSDB_FALLOCATE_PRESENT)
  endif()
endif()

CHECK_CXX_SOURCE_COMPILES("
#include <fcntl.h>
int main() {
  int fd = open(\"/dev/null\", 0);
  sync_file_range(fd, 0, 1024, SYNC_FILE_RANGE_WRITE);
}
" HAVE_SYNC_FILE_RANGE_WRITE)
if(HAVE_SYNC_FILE_RANGE_WRITE)
  add_definitions(-DROCKSDB_RANGESYNC_PRESENT)
endif()


include(CheckCXXSymbolExists)
check_cxx_symbol_exists(malloc_usable_size malloc.h HAVE_MALLOC_USABLE_SIZE)
if(HAVE_MALLOC_USABLE_SIZE)
  add_definitions(-DROCKSDB_MALLOC_USABLE_SIZE)
endif()

check_cxx_symbol_exists(sched_getcpu sched.h HAVE_SCHED_GETCPU)
if(HAVE_SCHED_GETCPU)
  add_definitions(-DROCKSDB_SCHED_GETCPU_PRESENT)
endif()



# add the binary tree to the search path for include files
# In GCC, this will invoke the "-I" command
# 添加当前 project 的根目录和 include 目录
include_directories(
  "${PROJECT_BINARY_DIR}/include"
  "${PROJECT_SOURCE_DIR}/src"
  "${PROJECT_BINARY_DIR}/src/sparsepp"
  "${PROJECT_SOURCE_DIR}/src/include"
)
# add system local lib
include_directories(/usr/local/include)

# For the static library:
link_directories( /usr/local/lib )

# add pthread support
find_package(Threads REQUIRED)
SET(CMAKE_CXX_FLAGS -pthread)



include(TestBigEndian)
test_big_endian(LEVELDB_IS_BIG_ENDIAN)

set (CMAKE_REQUIRED_INCLUDES "/usr/local/include")

include(CheckIncludeFile)
check_include_file("unistd.h" HAVE_UNISTD_H)

# include(CheckIncludeFileCXX)
# check_include_file_cxx("snappy.h" HAVE_SNAPPY)




# This will enable your add_test() under tests folder.
option(KV_BUILD_TESTS "Build KV's unit tests" ON)
enable_testing()
function(kv_test test_file)
  get_filename_component(test_target_name "${test_file}" NAME_WE)
  set (case_name test_${test_target_name})
  add_executable("${case_name}" "${test_file}")
  target_link_libraries("${case_name}" ${GTEST_LIBRARY} ${PROJECT_LINK_LIBS} db env util moni table mem port gflags)
  target_sources("${case_name}"
      PRIVATE
      "${test_file}"
  )
  add_test(NAME "${case_name}" COMMAND "${case_name}")
endfunction(kv_test)

function(kv_exe src_file)
  get_filename_component(target_name "${src_file}" NAME_WE)
  set (case_name ${target_name})
  add_executable("${case_name}" "${src_file}")
  target_link_libraries("${case_name}" ${GTEST_LIBRARY} ${PROJECT_LINK_LIBS} db env util moni table mem port gflags)
  target_sources("${case_name}"
      PRIVATE
      "${src_file}"
  )
endfunction(kv_exe)


# The add_subdirectory will add CMakeLists.txt under src and tests into your project.
add_subdirectory(src)
add_subdirectory(tests)


# Reference

# [Unit testing with GoogleTest and CMake](http://www.kaizou.org/2014/11/gtest-cmake/)
# [TDD FOR C++ IN CMAKE AND GOOGLETEST](https://youyue123.github.io/tech/2018/01/29/TDD-for-C++-in-CMake-And-GoogleTest.html)
# [External_GTest.cmake](https://gist.github.com/johnb003/65982fdc7a1274fdb023b0c68664ebe4)
