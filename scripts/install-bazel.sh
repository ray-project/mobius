#!/bin/bash
# Current github bazel version is 5.0.0
ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

arg1="${1-}"

achitecture="${HOSTTYPE}"
platform="unknown"
echo "platform is ${platform}"

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

if [ "${platform}" = "darwin" ]; then
    wget "https://github.com/bazelbuild/bazel/releases/download/5.0.0/bazel-5.0.0-installer-darwin-x86_64.sh" -O bazel-5.0.0-installer-darwin-x86_64.sh
    sh bazel-5.0.0-installer-darwin-x86_64.sh
else
    wget "https://github.com/bazelbuild/bazel/releases/download/5.0.0/bazel_5.0.0-linux-x86_64.deb" -O bazel_5.0.0-linux-x86_64.deb
    dpkg -i bazel_5.0.0-linux-x86_64.deb
fi
