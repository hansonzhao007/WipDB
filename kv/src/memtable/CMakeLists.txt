# CMAKELISTS.TXT UNDER mem

set(MEM_SRCS
    "alloc_tracker.cc"
    "hash_cuckoo_rep.cc"
    "hash_cuckoo_rep.h"
    "hash_skiplist_rep.cc"
    "hash_skiplist_rep.h"
    "inlineskiplist.h"
    "skiplistrep.cc"
    "stl_wrappers.h"
    "vectorrep.cc"
    "write_buffer_manager.cc"
)
# Declare the library
add_library(mem STATIC
    ${MEM_SRCS}
)
# Link dependency
# Link dependency
target_link_libraries(mem
    util port
)
# Specify here the include directories exported by this library
target_include_directories(mem PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)