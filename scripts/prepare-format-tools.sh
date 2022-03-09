#!/bin/bash

current_dir=$(dirname "${BASH_SOURCE:-$0}")
# Install format tools on ubuntu.

install_buildifier() {
    wget "https://ray-mobius-us.oss-us-west-1.aliyuncs.com/ci/linux/buildifier" -O $current_dir/buildifier
    export PATH=$PATH:$current_dir
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
