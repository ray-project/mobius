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
arm=`uname -m | grep arm`
echo $arm

if [ "${platform}" = "darwin" ] && [ "${arm}" != "arm64" ]; then
    wget "https://github.com/bazelbuild/bazel/releases/download/5.4.1/bazel-5.4.1-installer-darwin-x86_64.sh" -O bazel-5.4.1-installer-darwin-x86_64.sh
    sh bazel-5.4.1-installer-darwin-x86_64.sh
elif [ "${platform}" = "darwin" ] && [ "${arm}" = "arm64" ]; then
    wget "https://github.com/bazelbuild/bazel/releases/download/5.4.1/bazel-5.4.1-darwin-arm64" -O bazel-5.4.1-darwin-arm64
    chmod a+x bazel-5.4.1-darwin-arm64
    sh bazel-5.4.1-darwin-arm64
elif [ "${platform}" = "linux" ] && [ "${arm}" = "arm64" ]; then
    wget "https://github.com/bazelbuild/bazel/releases/download/5.4.1/bazel-5.4.1-linux-arm64" -O bazel_5.4.1-linux-arm64
    chmod a+x bazel_5.4.1-linux-arm64
    sh bazel_5.4.1-linux-arm64
else
    wget "https://github.com/bazelbuild/bazel/releases/download/5.4.1/bazel_5.4.1-linux-x86_64.deb" -O bazel_5.4.1-linux-x86_64.deb
    dpkg -i bazel_5.4.1-linux-x86_64.deb
fi

bazel --version
