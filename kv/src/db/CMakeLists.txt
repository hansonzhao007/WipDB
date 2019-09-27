# CMAKELISTS.TXT UNDER db

set(DB_SRCS
    "bucket.h"
    "builder.cc"
    "builder.h"
    "db_impl.cc"
    "db_impl.h"
    "db_iter.cc"
    "db_iter.h"
    "dbformat.cc"
    "dbformat.h"
    "filename.cc"
    "filename.h"
    "log_format.h"
    "log_reader.cc"
    "log_reader.h"
    "log_writer.cc"
    "log_writer.h"
    "kv_iter.cc"
    "kv_iter.h"
    "kv.cc"
    "memtable.cc"
    "memtable.h"
    "snapshot.h"
    "table_cache.cc"
    "table_cache.h"
    "version_edit.cc"
    "version_edit.h"
    "version_set.cc"
    "version_set.h"
    "versionkv.cc"
    "versionkv.h"
    "write_batch_internal.h"
    "write_batch.cc"
    "write_thread.cc"
    "write_thread.h"
)
# Declare the library
add_library(db STATIC
    ${DB_SRCS}
)
# Link dependency
target_link_libraries(db
    util table
)
# Specify here the include directories exported by this library
target_include_directories(db PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)