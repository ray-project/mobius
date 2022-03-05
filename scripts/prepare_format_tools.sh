#!/bin/bash

# Install format tools on ubuntu.

install_buildifier() {
    apt-get install golang-go
    export GO111MODULE=on
    go install github.com/bazelbuild/buildtools/buildifier@latest
}

install_clang_format() {
    pip3 install clang-format==12.0.0
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
