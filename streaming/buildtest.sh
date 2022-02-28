#!/bin/bash
script_dir=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)
function compile()                                                                                                                                                                     
{
    pushd $script_dir
      bazel build //:streaming_lib
    popd
}

function test_streaming_cpp() 
{
    pushd $script_dir
      #bazel test //:all --test_filter=basic
      # NOTE(lingxuan.zlx): unsupported host instruction of bazel on github workflow
      bazel test "streaming_message_ring_buffer_tests" "barrier_helper_tests" "streaming_message_serialization_tests" "streaming_mock_transfer" \
      "streaming_util_tests" "streaming_perf_tests" "event_service_tests" "queue_protobuf_tests" "data_writer_tests"
      exit $?
    popd
}

compile
test_streaming_cpp

