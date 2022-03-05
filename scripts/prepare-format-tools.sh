#!/bin/bash

# Install format tools on ubuntu.

install_buildifier() {
    pushd /
    git clone git@github.com:bazelbuild/buildtools.git
    cd bazelbuild
    export PATH=$PATH:/bazelbuild/bazel-bin/buildifier/buildifier_
    popd
}

install_clang_format() {
    pip3 install clang-format==12.0.1
}

install_black_format() {
    pip3 install black==21.12b0
}

install_shell_check() {
    apt-get install shellcheck
}

install_buildifier
install_clang_format
install_black_format
install_shell_check
