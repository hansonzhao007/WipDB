# CMAKELISTS.TXT UNDER env

set(ENV_SRCS
    "env_posix_test_helper.h"
    "env_posix.cc"
    "env.cc"
)
# Declare the library
add_library(env STATIC
    ${ENV_SRCS}
)
# Link dependency
# Link dependency
target_link_libraries(env
    util port
)
# Specify here the include directories exported by this library
target_include_directories(env PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)