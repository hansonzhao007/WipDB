# CMAKELISTS.TXT UNDER util

set(UTIL_SRCS
    "aligned_buffer.h"
    "allocator.h"
    "arena.cc"
    "arena.h"
    "autovector.h"
    "cache.cc"
    "cast_util.h"
    "coding.cc"
    "coding.h"
    "color.h"
    "comparator.cc"
    "core_local.h"
    "crc32c.cc"
    "crc32c.h"
    "debug.cc"
    "debug.h"
    "file_reader_writer.cc"
    "file_reader_writer.h"
    "generator.cc"
    "generator.h"
    "geninfo.h"
    "hash_table.cc"
    "hash_table.h"
    "hash.cc"
    "hash.h"
    "hashtable.cc"
    "hashtable.h"
    # "histogram.cc"
    # "histogram.h"
    "hpblock.cc"
    "hpblock.h"
    "iterator.cc"
    "murmurhash.cc"
    "murmurhash.h"
    # "logging.cc"
    # "logging.h"
    "mutexlock.h"
    "no_destructor.h"
    "options.cc"
    "perfsvg.cc"
    "perfsvg.h"
    "posix_logger.h"
    "random.cc"
    "random.h"
    "rate_limiter.cc"
    "rate_limiter.h"
    "skiplist.h"
    "slice.cc"
    "status.cc"
    "stop_watch.h"
    "string_util.cc"
    "string_util.h"
    "testharness.cc"
    "testharness.h"
    "testutil.cc"
    "testutil.h"
    "thread_local.cc"
    "thread_local.h"
    "thread_operation.h"
    "threadpool_imp.cc"
    "threadpool_imp.h"
    "trace.cc"
    "trace.h"
    "util.h"
)
set_source_files_properties(
    crc32c.cc
    PROPERTIES COMPILE_FLAGS "-msse4.2 -mpclmul")


# Declare the library
add_library(util STATIC
    ${UTIL_SRCS}
)
# Link dependency
target_link_libraries(util
    env
)
# Specify here the include directories exported by this library
target_include_directories(util PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)