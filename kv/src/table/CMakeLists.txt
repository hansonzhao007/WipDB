# CMAKELISTS.TXT UNDER db

set(Table_SRCS
    "block_builder.cc"
    "block.cc"
    "block.h"
    "bloom.cc"
    "filter_block.cc"
    "filter_block.h"
    "format.cc"
    "format.h"
    "iterator_wrapper.h"
    "merger.cc"
    "merger.h"
    "table_builder.cc"
    "table.cc"
    "two_level_iterator.cc"
    "two_level_iterator.h"
)
# Declare the library
add_library(table STATIC
    ${Table_SRCS}
)
# Link dependency
target_link_libraries(table
    util
)
# Specify here the include directories exported by this library
target_include_directories(table PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)