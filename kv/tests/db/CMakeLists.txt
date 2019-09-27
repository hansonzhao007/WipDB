file(GLOB test_cases  "*_test.cc")
file(GLOB cases  "*.cc")
list(REMOVE_ITEM cases ${test_cases})

foreach(case_file ${test_cases})
    kv_test(${case_file})
endforeach()

foreach(case ${cases})
    kv_exe(${case})
endforeach()