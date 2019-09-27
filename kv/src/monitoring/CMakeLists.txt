# CMAKELISTS.TXT UNDER env

set(MONI_SRCS
    "histogram.cc"
    "histogram.h"
    "instrumented_mutex.cc"
    "instrumented_mutex.h"
    "statistics.cc"
    "statistics.h"
    "thread_status_impl.cc"
    "thread_status_updater.cc"
    "thread_status_updater.h"
    "thread_status_util.cc"
    "thread_status_util.h"
)
# Declare the library
add_library(moni STATIC
    ${MONI_SRCS}
)
# Link dependency
# Link dependency
target_link_libraries(moni
    util port
)
# Specify here the include directories exported by this library
target_include_directories(moni PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)