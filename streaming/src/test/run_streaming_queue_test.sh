#!/usr/bin/env bash

# Run all streaming c++ tests using streaming queue, instead of plasma queue
# This needs to be run in the root directory.

# Try to find an unused port for raylet to use.
PORTS="2000 2001 2002 2003 2004 2005 2006 2007 2008 2009"
RAYLET_PORT=0
for port in $PORTS; do
    if ! nc -z localhost "$port"; then
        RAYLET_PORT=$port
        break
    fi
done

if [[ $RAYLET_PORT == 0 ]]; then
    echo "WARNING: Could not find unused port for raylet to use. Exiting without running tests."
    exit
fi

# Cause the script to exit if a single command fails.
set -e
set -x

# Get the directory in which this script is executing.
SCRIPT_DIR="$(dirname "$0")"

# Get the directory in which this script is executing.
SCRIPT_DIR="$(dirname "$0")"
STREAMING_ROOT="$SCRIPT_DIR/../.."
# Makes $STREAMING_ROOT an absolute path.
STREAMING_ROOT="$(cd "$STREAMING_ROOT" && pwd)"
if [ -z "$STREAMING_ROOT" ] ; then
  exit 1
fi

bazel build "@com_github_ray_project_ray//:mock_worker"  \
            "@com_github_ray_project_ray//:raylet" "@com_github_ray_project_ray//:gcs_server" \
            "@com_github_ray_project_ray//:redis-server" "@com_github_ray_project_ray//:redis-cli"
bazel build //:streaming_test_worker
bazel build //:streaming_queue_tests

# Ensure we're in the right directory.
if [ ! -d "$STREAMING_ROOT/python" ]; then
  echo "Unable to find root Ray directory. Has this script moved?"
  exit 1
fi

REDIS_SERVER_EXEC="$STREAMING_ROOT/bazel-bin/external/com_github_antirez_redis/redis-server"
REDIS_CLIENT_EXEC="$STREAMING_ROOT/bazel-bin/redis-cli"
RAYLET_EXEC="$STREAMING_ROOT/bazel-bin/raylet"
STREAMING_TEST_WORKER_EXEC="$STREAMING_ROOT/bazel-bin/streaming_test_worker"
GCS_SERVER_EXEC="$STREAMING_ROOT/bazel-bin/gcs_server"

# clear env
set +e
pgrep "plasma|DefaultDriver|DefaultWorker|AppStarter|redis|http_server|job_agent" | xargs kill -9 &> /dev/null
set -e

# Run tests.

# run all tests
"$STREAMING_ROOT"/bazel-bin/streaming_queue_tests "$RAYLET_EXEC" "$RAYLET_PORT" "$STREAMING_TEST_WORKER_EXEC" \
"$GCS_SERVER_EXEC" "$REDIS_SERVER_EXEC" "$REDIS_CLIENT_EXEC"
sleep 1s
