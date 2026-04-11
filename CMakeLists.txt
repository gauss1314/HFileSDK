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

# Detect compiler family
if(CMAKE_CXX_COMPILER_ID STREQUAL "Clang" OR
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

if(HFILE_PLATFORM_WINDOWS AND NOT HFILE_COMPILER_CLANG)
  message(FATAL_ERROR "Windows builds are supported only via MSYS2 clang/clang++ and the scripts/*.bat -> scripts/*.sh workflow")
endif()

# ─── Options ──────────────────────────────────────────────────────────────────
option(HFILE_ENABLE_TESTS      "Build test targets (requires GTest)" ON)
# io_uring is Linux-only; disable silently on other platforms
option(HFILE_ENABLE_IO_URING   "Enable io_uring async I/O (Linux only)" ON)
# libhdfs3 has no official Windows build
option(HFILE_ENABLE_HDFS       "Enable HDFS I/O via libhdfs3 (Linux/macOS only)" OFF)
option(HFILE_ENABLE_ASAN       "Enable AddressSanitizer" OFF)
option(HFILE_ENABLE_UBSAN      "Enable UndefinedBehaviorSanitizer" OFF)
option(HFILE_ENABLE_JEMALLOC   "Use jemalloc as memory allocator" OFF)
option(HFILE_ENABLE_COVERAGE   "Enable llvm-cov coverage instrumentation" OFF)
option(HFILE_ENABLE_JAVA_TESTS "Build JNI shared library and run Java integration tests" ON)
set(HFILE_COVERAGE_CTEST_ARGS "" CACHE STRING "Extra ctest arguments used by coverage targets")

# ─── Compiler flags ────────────────────────────────────────────────────────────
if(CMAKE_BUILD_TYPE STREQUAL "Release" AND NOT HFILE_ENABLE_COVERAGE)
  if(HFILE_COMPILER_GCC OR HFILE_COMPILER_CLANG)
    add_compile_options(-O3 -march=native)
    if(NOT APPLE)   # Apple's ld64 does not support -flto in the same way
      add_compile_options(-flto)
      add_link_options(-flto)
      # LTO produces IR bitcode .o files that /usr/bin/ar cannot index.
      # Use gcc-ar / gcc-ranlib (or llvm-ar / llvm-ranlib) which have
      # the LTO plugin and can build correct symbol tables in .a archives.
      if(HFILE_COMPILER_GCC)
        find_program(GCC_AR   NAMES gcc-ar   gcc-ar-${CMAKE_CXX_COMPILER_VERSION})
        find_program(GCC_RANLIB NAMES gcc-ranlib gcc-ranlib-${CMAKE_CXX_COMPILER_VERSION})
        if(GCC_AR AND GCC_RANLIB)
          set(CMAKE_AR     "${GCC_AR}"     CACHE FILEPATH "LTO-aware ar"     FORCE)
          set(CMAKE_RANLIB "${GCC_RANLIB}" CACHE FILEPATH "LTO-aware ranlib" FORCE)
        endif()
      elseif(HFILE_COMPILER_CLANG)
        find_program(LLVM_AR     NAMES llvm-ar)
        find_program(LLVM_RANLIB NAMES llvm-ranlib)
        if(LLVM_AR AND LLVM_RANLIB)
          set(CMAKE_AR     "${LLVM_AR}"     CACHE FILEPATH "LTO-aware ar"     FORCE)
          set(CMAKE_RANLIB "${LLVM_RANLIB}" CACHE FILEPATH "LTO-aware ranlib" FORCE)
        endif()
      endif()
    endif()
  else()
    message(FATAL_ERROR "Unsupported compiler for Release configuration")
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
  if(NOT HFILE_COMPILER_CLANG)
    message(FATAL_ERROR "HFILE_ENABLE_COVERAGE requires Clang or AppleClang")
  endif()
  find_program(HFILE_LLVM_COV_EXECUTABLE NAMES llvm-cov llvm-cov.exe)
  find_program(HFILE_LLVM_PROFDATA_EXECUTABLE NAMES llvm-profdata llvm-profdata.exe)
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

# Windows: define standard macros that the C library omits without this flag.
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
# zlib / zlib-ng — GZip compression for HFile blocks.
# zlib-ng in compat mode provides the same zlib.h API with SIMD-optimized
# deflate (typically 2-4x faster).  When installed in compat mode, zlib-ng
# replaces libz and find_package(ZLIB) picks it up automatically.
#
# To build with zlib-ng compat mode:
#   git clone https://github.com/zlib-ng/zlib-ng.git
#   cd zlib-ng && mkdir build && cd build
#   cmake .. -DZLIB_COMPAT=ON -DCMAKE_INSTALL_PREFIX=/usr/local
#   make -j && sudo make install
#
option(HFILE_PREFER_ZLIB_NG
       "Try to find zlib-ng (native API) before standard zlib" OFF)

if(HFILE_PREFER_ZLIB_NG)
  # zlib-ng native mode installs as 'zlib-ng' pkg-config package
  find_package(PkgConfig QUIET)
  if(PkgConfig_FOUND)
    pkg_check_modules(ZLIBNG QUIET zlib-ng)
  endif()
  if(ZLIBNG_FOUND)
    message(STATUS "Using zlib-ng native: ${ZLIBNG_LINK_LIBRARIES}")
    add_library(ZLIB::ZLIB ALIAS PkgConfig::ZLIBNG)
    set(HFILE_USING_ZLIB_NG TRUE)
  else()
    message(STATUS "zlib-ng not found via pkg-config, falling back to standard zlib")
    find_package(ZLIB REQUIRED)
  endif()
else()
  # Default: find_package(ZLIB) — works with both stock zlib and zlib-ng compat mode
  find_package(ZLIB REQUIRED)
endif()
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

if(NOT HFILE_ENABLE_TESTS)
  message(STATUS "Tests disabled (HFILE_ENABLE_TESTS=OFF). "
                 "Skip GTest dependency and all test targets.")
else()

# ── Windows / MSYS2: force GTest from the CLANG64 prefix ─────────────────────
#
# Root cause of repeated failures:
#   cmake.exe on MSYS2 is a NATIVE Windows process.  It does NOT understand
#   MSYS2 POSIX paths like /clang64.  When we called:
#       file(TO_CMAKE_PATH "$ENV{MSYSTEM_PREFIX}" ...)
#   with MSYSTEM_PREFIX=/clang64, cmake kept /clang64 as-is — it has no way
#   to resolve it to D:/msys64/clang64.  So GTest_DIR was set to a path cmake
#   could not find, and cmake silently fell back to the first GTest it could
#   find: D:/msys64/usr/lib (MSYS runtime, ABI-incompatible with LLVM libc++).
#
# Correct approach:
#   Derive the prefix exclusively from CMAKE_CXX_COMPILER, which cmake already
#   stores as a proper Windows path (D:/msys64/clang64/bin/clang++.exe).
#   Two get_filename_component(DIRECTORY) calls give D:/msys64/clang64.
#   This path works for any cmake operation without any further conversion.
#
# Additionally, build.sh passes -DGTest_DIR=<windows_path> via cygpath, which
# provides the highest-priority cmake -D override that defeats any stale cache.
if(HFILE_PLATFORM_WINDOWS)
  # ── Step 1: derive the CLANG64 prefix from the compiler path ───────────────
  # CMAKE_CXX_COMPILER is already a Windows path set by cmake's compiler probe.
  # Example: D:/msys64/clang64/bin/clang++.exe
  #   → DIRECTORY once → D:/msys64/clang64/bin
  #   → DIRECTORY twice → D:/msys64/clang64
  # This works regardless of how the compiler was specified and regardless of
  # MSYSTEM_PREFIX, MSYSTEM, or any other MSYS2 environment variable.
  get_filename_component(_MSYS2_PREFIX "${CMAKE_CXX_COMPILER}" DIRECTORY)
  get_filename_component(_MSYS2_PREFIX "${_MSYS2_PREFIX}"      DIRECTORY)
  message(STATUS "MSYS2 CLANG64 prefix (from compiler): ${_MSYS2_PREFIX}")

  # ── Step 2: clear ALL GTest cache variables ─────────────────────────────────
  # cmake caches results from both module-mode (GTEST_LIBRARY, etc.) and
  # config-mode (GTest_DIR) find_package calls.  Unsetting only GTest_DIR is
  # not enough — GTEST_LIBRARY and GTEST_MAIN_LIBRARY also cache the stale
  # /usr/lib path and cause cmake to skip re-searching entirely.
  foreach(_gv
      GTest_DIR        GTest_ROOT       GTEST_ROOT
      GTEST_INCLUDE_DIR  GTEST_INCLUDE_DIRS
      GTEST_LIBRARY      GTEST_LIBRARY_DEBUG    GTEST_LIBRARY_RELEASE
      GTEST_MAIN_LIBRARY GTEST_MAIN_LIBRARY_DEBUG GTEST_MAIN_LIBRARY_RELEASE)
    unset(${_gv} CACHE)
  endforeach()

  # ── Step 3: force GTest_DIR to the exact Windows path ──────────────────────
  # GTest_DIR is the highest-priority input for config-mode find_package.
  # Setting it here with FORCE overwrites any stale value in CMakeCache.txt
  # every time cmake re-configures (i.e. whenever CMakeLists.txt changes).
  # If build.sh also passes -DGTest_DIR on the command line (belt-and-suspenders),
  # the -D value takes precedence over this set() call.
  set(GTest_DIR "${_MSYS2_PREFIX}/lib/cmake/GTest"
      CACHE PATH "GTest cmake config dir (MSYS2 CLANG64, auto-derived)" FORCE)
  message(STATUS "GTest_DIR → ${GTest_DIR}")

  # Prepend the Windows prefix so all other find_package calls also prefer
  # CLANG64 packages over /usr packages.
  list(INSERT CMAKE_PREFIX_PATH 0 "${_MSYS2_PREFIX}")
endif()

# ── find GTest in CONFIG mode ─────────────────────────────────────────────────
# CONFIG keyword forces cmake to look for GTestConfig.cmake only, bypassing
# FindGTest.cmake (module mode).  Module mode caches GTEST_LIBRARY /
# GTEST_MAIN_LIBRARY separately and is harder to override reliably.
find_package(GTest REQUIRED CONFIG)

# ── ABI sanity check ─────────────────────────────────────────────────────────
if(HFILE_PLATFORM_WINDOWS AND TARGET GTest::gtest)
  get_target_property(_gt_loc GTest::gtest IMPORTED_LOCATION_RELEASE)
  if(NOT _gt_loc)
    get_target_property(_gt_loc GTest::gtest IMPORTED_LOCATION)
  endif()
  if(_gt_loc AND _gt_loc MATCHES "[/\\\\]usr[/\\\\]")
    message(FATAL_ERROR
      "\n"
      "====================================================================\n"
      " GTest found at WRONG location (MSYS runtime — ABI mismatch):\n"
      "   ${_gt_loc}\n"
      " Expected it under: ${_MSYS2_PREFIX}\n"
      "\n"
      " Fix: delete the build directory and rebuild from scratch:\n"
      "   scripts\\\\clean.bat   (from Windows CMD / build.bat)\n"
      "   sh scripts/clean.sh  (from MSYS2 terminal)\n"
      " then:\n"
      "   sh scripts/build.sh  (or scripts\\\\build.bat)\n"
      "====================================================================\n")
  else()
    message(STATUS "GTest ABI check OK: ${_gt_loc}")
  endif()
endif()

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
  test/test_memory_allocators.cc
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
    WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/tools/arrow-to-hfile)
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

endif() # HFILE_ENABLE_TESTS

if(HFILE_ENABLE_COVERAGE)
  if(NOT HFILE_ENABLE_TESTS)
    message(FATAL_ERROR "HFILE_ENABLE_COVERAGE requires HFILE_ENABLE_TESTS=ON")
  endif()
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
  set(HFILE_COVERAGE_TEST_BINARY_LINES "")
  foreach(TGT IN LISTS HFILE_TEST_TARGETS)
    string(APPEND HFILE_COVERAGE_TEST_BINARY_LINES "${TGT}|$<TARGET_FILE:${TGT}>\n")
  endforeach()
  file(GENERATE
    OUTPUT "${CMAKE_BINARY_DIR}/hfile_coverage_targets.cmake"
    CONTENT "set(HFILE_COVERAGE_OBJECTS\n${HFILE_COVERAGE_OBJECT_LINES})\n")
  file(GENERATE
    OUTPUT "${CMAKE_BINARY_DIR}/hfile_coverage_test_binaries.txt"
    CONTENT "${HFILE_COVERAGE_TEST_BINARY_LINES}")
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

message(STATUS "HFileSDK configuration:")
message(STATUS "  Build type:  ${CMAKE_BUILD_TYPE}")
message(STATUS "  Tests:       ${HFILE_ENABLE_TESTS}")
message(STATUS "  io_uring:    ${HFILE_ENABLE_IO_URING}")
message(STATUS "  HDFS:        ${HFILE_ENABLE_HDFS}")
message(STATUS "  jemalloc:    ${HFILE_ENABLE_JEMALLOC}")
message(STATUS "  ASan:        ${HFILE_ENABLE_ASAN}")
message(STATUS "  UBSan:       ${HFILE_ENABLE_UBSAN}")
message(STATUS "  Coverage:    ${HFILE_ENABLE_COVERAGE}")
message(STATUS "  Java tests:  ${HFILE_ENABLE_JAVA_TESTS}")
if(HFILE_ENABLE_COVERAGE)
  message(STATUS "  llvm-cov:    ${HFILE_LLVM_COV_EXECUTABLE}")
  message(STATUS "  llvm-profdata: ${HFILE_LLVM_PROFDATA_EXECUTABLE}")
endif()
