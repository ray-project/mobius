#!/bin/bash
# Current github bazel version is 5.0.0
wget "https://github.com/bazelbuild/bazel/releases/download/5.0.0/bazel_5.0.0-linux-x86_64.deb" -O bazel_5.0.0-linux-x86_64.deb 
dpkg -i bazel_5.0.0-linux-x86_64.deb
