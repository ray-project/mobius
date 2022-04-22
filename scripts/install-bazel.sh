#!/bin/bash
# Current github bazel version is 5.0.0
platform="unknown"

case "${OSTYPE}" in
  msys)
    echo "Platform is Windows."
    platform="windows"
    # No installer for Windows
    ;;
  darwin*)
    echo "Platform is Mac OS X."
    platform="darwin"
    ;;
  linux*)
    echo "Platform is Linux (or WSL)."
    platform="linux"
    ;;
  *)
    echo "Platform is Linux (or WSL)."
    platform="linux"
    ;;
esac
echo "platform is ${platform}"

if [ "${platform}" = "darwin" ]; then
    wget "https://github.com/bazelbuild/bazel/releases/download/5.1.0/bazel-5.1.0-installer-darwin-x86_64.sh" -O bazel-5.1.0-installer-darwin-x86_64.sh
    sh bazel-5.1.0-installer-darwin-x86_64.sh
else
    wget "https://github.com/bazelbuild/bazel/releases/download/5.1.0/bazel_5.1.0-linux-x86_64.deb" -O bazel_5.1.0-linux-x86_64.deb
    dpkg -i bazel_5.1.0-linux-x86_64.deb
fi

bazel --version
