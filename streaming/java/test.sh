#!/usr/bin/env bash

# Cause the script to exit if a single command fails.
set -e
# Show explicitly which commands are currently running.
set -x

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
OUTPUT_DIR="/tmp/ray_streaming_java_test_output"

function zip_and_upload_log() {
    bash "$ROOT_DIR"/../../scripts/ossutils.sh zip_dir_and_upload $1 java-test-log /ci/logs
}

pushd "$ROOT_DIR"
echo "Check java code format."
# check google java style
mvn -T16 spotless:check
# check naming and others
mvn -T16 checkstyle:check
popd

echo "Build ray streaming"
bazel build @com_github_ray_streaming//java:all

echo "Running streaming tests."
java -cp "$ROOT_DIR"/../bazel-bin/java/all_streaming_tests_deploy.jar\
 org.testng.TestNG -d /tmp/ray_streaming_java_test_output "$ROOT_DIR"/testng.xml ||
exit_code=$?
if [ -z ${exit_code+x} ]; then
  exit_code=0
fi

echo "Collect and upload streaming testNG results."
if [ -f "${OUTPUT_DIR}/testng-results.xml" ] ; then
  zip_and_upload_log "${OUTPUT_DIR}"
else
  echo "Test result file doesn't exist."
fi

# exit_code == 2 means there are skipped tests.
if [ $exit_code -ne 2 ] && [ $exit_code -ne 0 ] ; then
    if [ -d "${OUTPUT_DIR}" ] ; then
      echo "Output abnormal test results:"
      for f in "${OUTPUT_DIR}"/*.{log,xml}; do
        if [ -f "$f" ]; then
          cat "$f"
        elif [[ -d $f ]]; then
          echo "$f is a directory"
        fi
      done
    fi

    for f in "${ROOT_DIR}"/hs_err*log; do
      if [ -f "$f" ]; then
        cat "$f"
      fi
    done
    exit $exit_code
fi

echo "Maven install ray streaming。"
pushd "$ROOT_DIR"
mvn -Dorg.slf4j.simpleLogger.defaultLogLevel=WARN clean install -DskipTests -Dcheckstyle.skip
