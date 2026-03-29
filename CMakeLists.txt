cmake_minimum_required(VERSION 3.20)
project(HFileSDK VERSION 1.0.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ─── Platform detection ────────────────────────────────────────────────────────
if(WIN32)
  set(HFILE_PLATFORM_WINDOWS ON)
  message(STATUS "Platform: Windows")
elseif(APPLE)
  set(HFILE_PLATFORM_MACOS ON)
  message(STATUS "Platform: macOS")
else()
  set(HFILE_PLATFORM_LINUX ON)
  message(STATUS "Platform: Linux")
endif()

# Detect Clang-cl (Clang targeting MSVC ABI on Windows)
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" AND MSVC)
  set(HFILE_COMPILER_CLANG_CL ON)
  message(STATUS "Compiler: Clang-cl")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR
       CMAKE_CXX_COMPILER_ID STREQUAL "AppleClang")
  set(HFILE_COMPILER_CLANG ON)
  message(STATUS "Compiler: ${CMAKE_CXX_COMPILER_ID}")
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
  set(HFILE_COMPILER_GCC ON)
  message(STATUS "Compiler: GCC")
elseif(MSVC)
  set(HFILE_COMPILER_MSVC ON)
  message(STATUS "Compiler: MSVC")
endif()

# ─── Options ──────────────────────────────────────────────────────────────────
option(HFILE_ENABLE_BENCHMARKS "Build benchmark targets" OFF)
# io_uring is Linux-only; disable silently on other platforms
option(HFILE_ENABLE_IO_URING   "Enable io_uring async I/O (Linux only)" ON)
# libhdfs3 has no official Windows build
option(HFILE_ENABLE_HDFS       "Enable HDFS I/O via libhdfs3 (Linux/macOS only)" OFF)
option(HFILE_ENABLE_ASAN       "Enable AddressSanitizer" OFF)
option(HFILE_ENABLE_UBSAN      "Enable UndefinedBehaviorSanitizer" OFF)
option(HFILE_ENABLE_JEMALLOC   "Use jemalloc as memory allocator" OFF)
option(HFILE_ENABLE_COVERAGE   "Enable llvm-cov coverage instrumentation" OFF)
option(HFILE_ENABLE_JAVA_TESTS "Build JNI shared library and run Java integration tests" ON)

# ─── Compiler flags ────────────────────────────────────────────────────────────
if(CMAKE_BUILD_TYPE STREQUAL "Release" AND NOT HFILE_ENABLE_COVERAGE)
  if(HFILE_COMPILER_MSVC)
    # MSVC / Clang-cl
    add_compile_options(/O2 /GL)
    add_link_options(/LTCG)
  elseif(HFILE_COMPILER_CLANG_CL)
    add_compile_options(/O2 -march=native)
    add_link_options(/LTCG)
  else()
    # GCC / Clang on Linux or macOS
    add_compile_options(-O3 -march=native)
    if(NOT APPLE)   # Apple's ld64 does not support -flto in the same way
      add_compile_options(-flto)
      add_link_options(-flto)
    endif()
  endif()
endif()

if(HFILE_ENABLE_ASAN AND NOT HFILE_PLATFORM_WINDOWS)
  add_compile_options(-fsanitize=address -fno-omit-frame-pointer)
  add_link_options(-fsanitize=address)
endif()

if(HFILE_ENABLE_UBSAN AND NOT HFILE_PLATFORM_WINDOWS)
  add_compile_options(-fsanitize=undefined)
  add_link_options(-fsanitize=undefined)
endif()

if(HFILE_ENABLE_COVERAGE)
  if(HFILE_PLATFORM_WINDOWS)
    message(FATAL_ERROR "HFILE_ENABLE_COVERAGE is supported on macOS/Linux only")
  endif()
  if(NOT HFILE_COMPILER_CLANG)
    message(FATAL_ERROR "HFILE_ENABLE_COVERAGE requires Clang or AppleClang")
  endif()
  find_program(HFILE_LLVM_COV_EXECUTABLE NAMES llvm-cov)
  find_program(HFILE_LLVM_PROFDATA_EXECUTABLE NAMES llvm-profdata)
  if(APPLE AND (NOT HFILE_LLVM_COV_EXECUTABLE OR NOT HFILE_LLVM_PROFDATA_EXECUTABLE))
    execute_process(
      COMMAND xcrun --find llvm-cov
      OUTPUT_VARIABLE HFILE_XCRUN_LLVM_COV
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET
    )
    execute_process(
      COMMAND xcrun --find llvm-profdata
      OUTPUT_VARIABLE HFILE_XCRUN_LLVM_PROFDATA
      OUTPUT_STRIP_TRAILING_WHITESPACE
      ERROR_QUIET
    )
    if(NOT HFILE_LLVM_COV_EXECUTABLE AND HFILE_XCRUN_LLVM_COV)
      set(HFILE_LLVM_COV_EXECUTABLE "${HFILE_XCRUN_LLVM_COV}")
    endif()
    if(NOT HFILE_LLVM_PROFDATA_EXECUTABLE AND HFILE_XCRUN_LLVM_PROFDATA)
      set(HFILE_LLVM_PROFDATA_EXECUTABLE "${HFILE_XCRUN_LLVM_PROFDATA}")
    endif()
  endif()
  if(NOT HFILE_LLVM_COV_EXECUTABLE)
    message(FATAL_ERROR "llvm-cov not found")
  endif()
  if(NOT HFILE_LLVM_PROFDATA_EXECUTABLE)
    message(FATAL_ERROR "llvm-profdata not found")
  endif()
  add_compile_options(-O0 -g -fprofile-instr-generate -fcoverage-mapping)
  add_link_options(-fprofile-instr-generate -fcoverage-mapping)
endif()

# Enable BlockPool double-release / ownership checks in Debug builds.
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_compile_definitions(HFILE_DEBUG_POOL)
endif()

# Windows: suppress MSVC-style warnings when using Clang-cl, and define
# standard macros that the C library omits without this flag.
if(HFILE_PLATFORM_WINDOWS)
  add_compile_definitions(
    _CRT_SECURE_NO_WARNINGS    # silence fopen deprecation
    NOMINMAX                   # don't let windows.h stomp std::min/max
    WIN32_LEAN_AND_MEAN
  )
endif()

# Find dependencies
find_package(Arrow REQUIRED)
find_package(Protobuf REQUIRED)
find_package(ZLIB REQUIRED)
find_package(Threads REQUIRED)

if(HFILE_ENABLE_JAVA_TESTS)
  find_package(JNI)
  find_program(HFILE_MAVEN_EXECUTABLE NAMES mvn mvn.cmd)
  if(NOT JNI_FOUND OR NOT HFILE_MAVEN_EXECUTABLE)
    message(WARNING "Java JNI tests disabled: JNI headers or Maven not found")
    set(HFILE_ENABLE_JAVA_TESTS OFF)
  endif()
endif()

find_library(ZSTD_LIB zstd REQUIRED)
find_library(LZ4_LIB lz4 REQUIRED)
find_library(SNAPPY_LIB snappy REQUIRED)

find_path(ZSTD_INCLUDE_DIR zstd.h)
find_path(LZ4_INCLUDE_DIR lz4.h)
find_path(SNAPPY_INCLUDE_DIR snappy.h)

# ─── io_uring: Linux only ─────────────────────────────────────────────────────
if(HFILE_ENABLE_IO_URING AND NOT HFILE_PLATFORM_LINUX)
  message(STATUS "io_uring: disabled (Linux only, current platform is not Linux)")
  set(HFILE_ENABLE_IO_URING OFF)
endif()

if(HFILE_ENABLE_IO_URING)
  find_library(URING_LIB uring)
  find_path(URING_INCLUDE_DIR liburing.h)
  if(URING_LIB AND URING_INCLUDE_DIR)
    message(STATUS "io_uring found: ${URING_LIB}")
    add_compile_definitions(HFILE_HAS_IO_URING)
  else()
    message(WARNING "io_uring not found, disabling")
    set(HFILE_ENABLE_IO_URING OFF)
  endif()
endif()

# ─── HDFS: no official Windows build of libhdfs3 ─────────────────────────────
if(HFILE_ENABLE_HDFS AND HFILE_PLATFORM_WINDOWS)
  message(STATUS "HDFS: disabled (libhdfs3 has no official Windows build)")
  set(HFILE_ENABLE_HDFS OFF)
endif()

if(HFILE_ENABLE_HDFS)
  find_library(HDFS3_LIB hdfs3)
  find_path(HDFS3_INCLUDE_DIR hdfs/hdfs.h)
  if(HDFS3_LIB AND HDFS3_INCLUDE_DIR)
    message(STATUS "libhdfs3 found: ${HDFS3_LIB}")
    add_compile_definitions(HFILE_HAS_HDFS)
  else()
    message(WARNING "libhdfs3 not found, disabling HDFS support")
    set(HFILE_ENABLE_HDFS OFF)
  endif()
endif()

if(HFILE_ENABLE_JEMALLOC)
  find_library(JEMALLOC_LIB jemalloc)
  if(JEMALLOC_LIB)
    message(STATUS "jemalloc found: ${JEMALLOC_LIB}")
    add_compile_definitions(HFILE_HAS_JEMALLOC)
  endif()
endif()

# Generate protobuf sources
set(PROTO_DIR ${CMAKE_SOURCE_DIR}/proto)
set(PROTO_FILES
  ${PROTO_DIR}/hfile_trailer.proto
  ${PROTO_DIR}/hfile_file_info.proto)

protobuf_generate_cpp(PROTO_SRCS PROTO_HDRS ${PROTO_FILES})
message(STATUS "Protobuf generated: ${PROTO_SRCS} ${PROTO_HDRS}")

# Library sources
set(HFILE_SOURCES
  src/checksum/crc32c.cc
  src/memory/arena_allocator.cc
  src/memory/block_pool.cc
  src/memory/memory_budget.cc
  src/codec/compressor.cc
  src/block/none_encoder.cc
  src/block/prefix_encoder.cc
  src/block/diff_encoder.cc
  src/block/fast_diff_encoder.cc
  src/block/block_builder.cc
  src/index/block_index_writer.cc
  src/bloom/compound_bloom_filter_writer.cc
  src/meta/file_info_builder.cc
  src/meta/trailer_builder.cc
  src/io/buffered_writer.cc
  src/io/atomic_file_writer.cc
  src/metrics/metrics_registry.cc
  src/partition/region_partitioner.cc
  src/partition/cf_grouper.cc
  src/arrow/row_key_builder.cc
  src/arrow/arrow_to_kv_converter.cc
  src/convert/converter.cc
  src/writer.cc
  src/bulk_load_writer.cc
  ${PROTO_SRCS}
)

if(HFILE_ENABLE_IO_URING)
  list(APPEND HFILE_SOURCES src/io/iouring_writer.cc)
endif()

if(HFILE_ENABLE_HDFS)
  list(APPEND HFILE_SOURCES src/io/hdfs_writer.cc)
endif()

add_library(hfile STATIC ${HFILE_SOURCES})
set_target_properties(hfile PROPERTIES POSITION_INDEPENDENT_CODE ON)

target_include_directories(hfile
  PUBLIC
    $<BUILD_INTERFACE:${CMAKE_SOURCE_DIR}/include>
    $<INSTALL_INTERFACE:include>
  PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}   # for generated protobuf headers
    ${ZSTD_INCLUDE_DIR}
    ${LZ4_INCLUDE_DIR}
    ${SNAPPY_INCLUDE_DIR}
    ${Protobuf_INCLUDE_DIRS}
)

target_link_libraries(hfile
  PUBLIC
    Arrow::arrow_shared
    protobuf::libprotobuf
    Threads::Threads
  PRIVATE
    ${ZSTD_LIB}
    ${LZ4_LIB}
    ${SNAPPY_LIB}
    ZLIB::ZLIB
)

if(HFILE_ENABLE_IO_URING)
  target_link_libraries(hfile PRIVATE ${URING_LIB})
  target_include_directories(hfile PRIVATE ${URING_INCLUDE_DIR})
endif()

if(HFILE_ENABLE_HDFS)
  target_link_libraries(hfile PRIVATE ${HDFS3_LIB})
  target_include_directories(hfile PRIVATE ${HDFS3_INCLUDE_DIR})
endif()

if(HFILE_ENABLE_JEMALLOC AND JEMALLOC_LIB)
  target_link_libraries(hfile PRIVATE ${JEMALLOC_LIB})
endif()

if(HFILE_ENABLE_JAVA_TESTS)
  get_filename_component(ZSTD_LIB_DIR ${ZSTD_LIB} DIRECTORY)
  get_filename_component(LZ4_LIB_DIR ${LZ4_LIB} DIRECTORY)
  get_filename_component(SNAPPY_LIB_DIR ${SNAPPY_LIB} DIRECTORY)
  add_library(hfilesdk SHARED src/jni/hfile_jni.cc)
  set_target_properties(hfilesdk PROPERTIES
    OUTPUT_NAME hfilesdk
    LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
    BUILD_RPATH "$<TARGET_FILE_DIR:Arrow::arrow_shared>;$<TARGET_FILE_DIR:protobuf::libprotobuf>;$<TARGET_FILE_DIR:ZLIB::ZLIB>;${ZSTD_LIB_DIR};${LZ4_LIB_DIR};${SNAPPY_LIB_DIR}")
  target_link_libraries(hfilesdk PRIVATE hfile)
  if(TARGET JNI::JNI)
    target_link_libraries(hfilesdk PRIVATE JNI::JNI)
  endif()
  target_include_directories(hfilesdk PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR}
    ${JNI_INCLUDE_DIRS})
endif()

add_executable(hfile_chaos tools/hfile-chaos/main.cc)
set_target_properties(hfile_chaos PROPERTIES OUTPUT_NAME hfile-chaos)
target_link_libraries(hfile_chaos PRIVATE hfile)
target_include_directories(hfile_chaos PRIVATE
  ${CMAKE_SOURCE_DIR}/src
  ${CMAKE_BINARY_DIR})

# Tests
enable_testing()
find_package(GTest REQUIRED)
set(HFILE_TEST_TARGETS)

set(GTEST_SOURCES
  test/test_crc32c.cc
  test/test_kv_encoding.cc
  test/test_block_builder.cc
  test/test_bloom_filter.cc
  test/test_file_info.cc
  test/test_trailer.cc
  test/test_hfile_writer.cc
  test/test_prefix_encoder.cc
  test/test_diff_encoder.cc
  test/test_fast_diff_encoder.cc
  test/test_index_writer.cc
  test/test_compressor.cc
  test/test_arrow_converter.cc
  test/test_region_partitioner.cc
  test/test_cf_grouper.cc
  test/test_io_writers.cc
  test/test_jni_json_utils.cc
  test/test_bulk_load_writer_behavior.cc
)

foreach(TEST_SRC ${GTEST_SOURCES})
  get_filename_component(TEST_NAME ${TEST_SRC} NAME_WE)
  add_executable(${TEST_NAME} ${TEST_SRC})
  target_link_libraries(${TEST_NAME}
    PRIVATE hfile GTest::gtest GTest::gtest_main)
  target_include_directories(${TEST_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR})
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
  list(APPEND HFILE_TEST_TARGETS ${TEST_NAME})
endforeach()

set(CUSTOM_TEST_SOURCES
  test/test_row_key_builder.cc
  test/test_production_features.cc
  test/test_round3_bugs.cc
)

foreach(TEST_SRC ${CUSTOM_TEST_SOURCES})
  get_filename_component(TEST_NAME ${TEST_SRC} NAME_WE)
  add_executable(${TEST_NAME} ${TEST_SRC})
  target_link_libraries(${TEST_NAME} PRIVATE hfile)
  target_include_directories(${TEST_NAME} PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_BINARY_DIR})
  add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
  list(APPEND HFILE_TEST_TARGETS ${TEST_NAME})
endforeach()

add_test(
  NAME hfile_chaos_kill
  COMMAND hfile_chaos
    --mode kill-during-write
    --output-dir ${CMAKE_BINARY_DIR}/chaos/kill
    --verify-no-corrupt-files
    --rows 50000
    --value-size 2048
)

add_test(
  NAME hfile_chaos_disk_full
  COMMAND hfile_chaos
    --mode disk-full-sim
    --output-dir ${CMAKE_BINARY_DIR}/chaos/disk
    --verify-no-corrupt-files
)

if(HFILE_ENABLE_JAVA_TESTS)
  add_test(
    NAME test_java_jni_integration
    COMMAND ${CMAKE_COMMAND} -E env
      HFILESDK_NATIVE_DIR=$<TARGET_FILE_DIR:hfilesdk>
      HFILESDK_NATIVE_LIB=$<TARGET_FILE:hfilesdk>
      HFILESDK_BUILD_DIR=${CMAKE_BINARY_DIR}
      ${HFILE_MAVEN_EXECUTABLE} -q test
  )
  set_tests_properties(test_java_jni_integration PROPERTIES
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/java)
  add_test(
    NAME test_java_hbase_reader_verify
    COMMAND ${CMAKE_COMMAND}
      -DHFILE_SOURCE_DIR=${CMAKE_SOURCE_DIR}
      -DHFILE_BUILD_DIR=${CMAKE_BINARY_DIR}
      -DHFILE_MAVEN_EXECUTABLE=${HFILE_MAVEN_EXECUTABLE}
      -DHFILE_NATIVE_DIR=$<TARGET_FILE_DIR:hfilesdk>
      -DHFILE_NATIVE_LIB=$<TARGET_FILE:hfilesdk>
      -P ${CMAKE_SOURCE_DIR}/cmake/run_java_hbase_verify.cmake
  )
endif()

if(HFILE_ENABLE_COVERAGE)
  set(HFILE_COVERAGE_TARGETS ${HFILE_TEST_TARGETS} hfile_chaos)
  set(HFILE_COVERAGE_OBJECT_TARGETS hfile hfile_chaos)
  if(HFILE_ENABLE_JAVA_TESTS)
    list(APPEND HFILE_COVERAGE_TARGETS hfilesdk)
    list(APPEND HFILE_COVERAGE_OBJECT_TARGETS hfilesdk)
  else()
    list(APPEND HFILE_COVERAGE_OBJECT_TARGETS test_jni_json_utils)
  endif()
  set(HFILE_COVERAGE_OBJECT_LINES "")
  foreach(TGT IN LISTS HFILE_COVERAGE_OBJECT_TARGETS)
    string(APPEND HFILE_COVERAGE_OBJECT_LINES "  \"$<TARGET_FILE:${TGT}>\"\n")
  endforeach()
  file(GENERATE
    OUTPUT "${CMAKE_BINARY_DIR}/hfile_coverage_targets.cmake"
    CONTENT "set(HFILE_COVERAGE_OBJECTS\n${HFILE_COVERAGE_OBJECT_LINES})\n")
  configure_file(
    "${CMAKE_SOURCE_DIR}/cmake/llvm_cov_report.cmake.in"
    "${CMAKE_BINARY_DIR}/llvm_cov_report.cmake"
    @ONLY)
  add_custom_target(hfile_coverage
    COMMAND "${CMAKE_COMMAND}" -P "${CMAKE_BINARY_DIR}/llvm_cov_report.cmake"
    DEPENDS ${HFILE_COVERAGE_TARGETS}
    USES_TERMINAL)
  add_custom_target(hfile_coverage_ci
    COMMAND "${CMAKE_COMMAND}" -P "${CMAKE_BINARY_DIR}/llvm_cov_report.cmake"
    DEPENDS ${HFILE_COVERAGE_TARGETS}
    USES_TERMINAL)
  add_custom_target(hfile_coverage_clean
    COMMAND "${CMAKE_COMMAND}" -E rm -rf "${CMAKE_BINARY_DIR}/coverage"
    COMMAND "${CMAKE_COMMAND}" -E rm -rf "${CMAKE_BINARY_DIR}/artifacts")
endif()

# Benchmarks
if(HFILE_ENABLE_BENCHMARKS)
  find_package(benchmark REQUIRED)

  set(BENCH_SOURCES
    bench/micro/bm_kv_encode.cc
    bench/micro/bm_crc32c.cc
    bench/micro/bm_compress.cc
    bench/micro/bm_bloom.cc
    bench/micro/bm_arrow_to_kv.cc
    bench/macro/bm_e2e_write.cc
  )

  foreach(BENCH_SRC ${BENCH_SOURCES})
    get_filename_component(BENCH_NAME ${BENCH_SRC} NAME_WE)
    add_executable(${BENCH_NAME} ${BENCH_SRC})
    target_link_libraries(${BENCH_NAME}
      PRIVATE hfile benchmark::benchmark benchmark::benchmark_main)
    target_include_directories(${BENCH_NAME} PRIVATE
      ${CMAKE_SOURCE_DIR}/src
      ${CMAKE_BINARY_DIR})
  endforeach()
endif()

message(STATUS "HFileSDK configuration:")
message(STATUS "  Build type:  ${CMAKE_BUILD_TYPE}")
message(STATUS "  io_uring:    ${HFILE_ENABLE_IO_URING}")
message(STATUS "  HDFS:        ${HFILE_ENABLE_HDFS}")
message(STATUS "  Benchmarks:  ${HFILE_ENABLE_BENCHMARKS}")
message(STATUS "  jemalloc:    ${HFILE_ENABLE_JEMALLOC}")
message(STATUS "  ASan:        ${HFILE_ENABLE_ASAN}")
message(STATUS "  UBSan:       ${HFILE_ENABLE_UBSAN}")
message(STATUS "  Coverage:    ${HFILE_ENABLE_COVERAGE}")
message(STATUS "  Java tests:  ${HFILE_ENABLE_JAVA_TESTS}")
if(HFILE_ENABLE_COVERAGE)
  message(STATUS "  llvm-cov:    ${HFILE_LLVM_COV_EXECUTABLE}")
  message(STATUS "  llvm-profdata: ${HFILE_LLVM_PROFDATA_EXECUTABLE}")
endif()
