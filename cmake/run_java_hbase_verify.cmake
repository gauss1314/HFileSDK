if(NOT DEFINED HFILE_SOURCE_DIR OR
   NOT DEFINED HFILE_BUILD_DIR OR
   NOT DEFINED HFILE_MAVEN_EXECUTABLE OR
   NOT DEFINED HFILE_NATIVE_DIR OR
   NOT DEFINED HFILE_NATIVE_LIB)
  message(FATAL_ERROR "Missing required variables for Java/HBase verification")
endif()

set(HFILE_FIXTURE_DIR "${HFILE_BUILD_DIR}/java-reader-fixture")
set(HFILE_ARROW_PATH "${HFILE_FIXTURE_DIR}/reader-input.arrow")
set(HFILE_HFILE_PATH "${HFILE_FIXTURE_DIR}/reader-output.hfile")
set(HFILE_VERIFY_MODULE_DIR "${HFILE_SOURCE_DIR}/tools/hfile-verify")
set(HFILE_VERIFY_TARGET_DIR "${HFILE_VERIFY_MODULE_DIR}/target")

file(REMOVE_RECURSE "${HFILE_FIXTURE_DIR}")
file(MAKE_DIRECTORY "${HFILE_FIXTURE_DIR}")

execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env
          HFILESDK_NATIVE_DIR=${HFILE_NATIVE_DIR}
          HFILESDK_NATIVE_LIB=${HFILE_NATIVE_LIB}
          HFILESDK_BUILD_DIR=${HFILE_BUILD_DIR}
          "${HFILE_MAVEN_EXECUTABLE}" -q
          "-Dtest=HFileSDKIntegrationTest#convertWritesReaderValidationFixture"
          "-Dhfilesdk.e2e.arrowPath=${HFILE_ARROW_PATH}"
          "-Dhfilesdk.e2e.hfilePath=${HFILE_HFILE_PATH}"
          test
  WORKING_DIRECTORY "${HFILE_SOURCE_DIR}/tools/arrow-to-hfile"
  RESULT_VARIABLE HFILE_JAVA_TEST_STATUS
)

if(NOT HFILE_JAVA_TEST_STATUS EQUAL 0)
  message(FATAL_ERROR "Java JNI fixture generation failed")
endif()

execute_process(
  COMMAND "${HFILE_MAVEN_EXECUTABLE}" -q -DskipTests package
  WORKING_DIRECTORY "${HFILE_VERIFY_MODULE_DIR}"
  RESULT_VARIABLE HFILE_VERIFY_PACKAGE_STATUS
)

if(NOT HFILE_VERIFY_PACKAGE_STATUS EQUAL 0)
  message(FATAL_ERROR "Packaging hfile-verify failed")
endif()

file(GLOB HFILE_VERIFY_JARS "${HFILE_VERIFY_TARGET_DIR}/hfile-verify-*.jar")
list(FILTER HFILE_VERIFY_JARS EXCLUDE REGEX ".*/original-.*\\.jar$")
list(LENGTH HFILE_VERIFY_JARS HFILE_VERIFY_JAR_COUNT)
if(HFILE_VERIFY_JAR_COUNT EQUAL 0)
  message(FATAL_ERROR "Could not find packaged hfile-verify jar")
endif()
list(GET HFILE_VERIFY_JARS 0 HFILE_VERIFY_JAR)

execute_process(
  COMMAND java -jar "${HFILE_VERIFY_JAR}"
          --hfile "${HFILE_HFILE_PATH}"
          --expect-major-version 3
          --expect-entry-count 4
          --expect-compression NONE
          --expect-encoding NONE
          --expect-rows row1,row1,row2,row2
          --expect-families cf,cf,cf,cf
          --expect-qualifiers id,value,id,value
          --expect-values row1,value1,row2,value2
          --expect-types Put,Put,Put,Put
  WORKING_DIRECTORY "${HFILE_VERIFY_MODULE_DIR}"
  RESULT_VARIABLE HFILE_VERIFY_STATUS
)

if(NOT HFILE_VERIFY_STATUS EQUAL 0)
  message(FATAL_ERROR "HBase reader verification failed")
endif()
