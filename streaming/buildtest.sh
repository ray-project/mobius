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
      bazel test //:all
    popd
}

#compile
test_streaming_cpp

