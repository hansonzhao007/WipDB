# CMAKELISTS.TXT UNDER port

set(Port_SRCS
    "port_posix.cc"
)
# Declare the library
add_library(port STATIC
    ${Port_SRCS}
)

# Specify here the include directories exported by this library
target_include_directories(port PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)