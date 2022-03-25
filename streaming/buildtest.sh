#!/bin/bash
script_dir=$(cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)

function compile()
{
    pushd "$script_dir" || exit
      bazel build //:streaming_lib
    popd || exit
}

function test_streaming_cpp() 
{
    pushd "$script_dir" || exit

    #bazel test //:all --test_filter=basic
    # NOTE(lingxuan.zlx): unsupported host instruction of bazel on github workflow
    bazel test "streaming_message_ring_buffer_tests" "barrier_helper_tests" "streaming_message_serialization_tests" "streaming_mock_transfer" \
    "streaming_util_tests" "streaming_perf_tests" "event_service_tests" "queue_protobuf_tests" "data_writer_tests" "buffer_pool_tests"
    exit $?

    popd || exit
}

# To shorten compile time we disable compile before cpp test.
#compile
test_streaming_cpp

