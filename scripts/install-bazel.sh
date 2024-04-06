#!/bin/bash
# Current github bazel version is 5.0.0
platform="unknown"
arm=`uname -a | grep -o -m 1 -e "arm" -e "aarch64" | head -n 1`

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
echo "current arch is $arm"

if [ "${arm}" = "aarch64" ]; then
    arm="arm64"
fi

bazel_exe_file=""

if [ "${platform}" = "darwin" ] && [ "${arm}" != "arm64" ]; then
    bazel_exe_file="bazel-5.4.1-installer-darwin-x86_64.sh"
    wget "https://github.com/bazelbuild/bazel/releases/download/5.4.1/bazel-5.4.1-installer-darwin-x86_64.sh" -O $bazel_exe_file
    sh $bazel_exe_file
elif [ "${platform}" = "darwin" ] && [ "${arm}" = "arm64" ]; then
    wget "https://github.com/bazelbuild/bazel/releases/download/5.4.1/bazel-5.4.1-darwin-arm64" -O bazel-5.4.1-darwin-arm64
    chmod a+x bazel-5.4.1-darwin-arm64
    cp bazel-5.4.1-darwin-arm64 /usr/bin/bazel
elif [ "${platform}" = "linux" ] && [ "${arm}" = "arm64" ]; then
    bazel_exe_file="bazel_5.4.1-linux-arm64"
    wget "https://github.com/bazelbuild/bazel/releases/download/5.4.1/bazel-5.4.1-linux-arm64" -O $bazel_exe_file
    chmod a+x $bazel_exe_file
    cp $bazel_exe_file /usr/bin/bazel
else
    bazel_exe_file="bazel_5.4.1-linux-x86_64.deb"
    wget "https://github.com/bazelbuild/bazel/releases/download/5.4.1/bazel_5.4.1-linux-x86_64.deb" -O $bazel_exe_file
    dpkg -i $bazel_exe_file
fi

if [ -e $bazel_exe_file ]; then
  echo "Remove download file $bazel_exe_file"
  rm $bazel_exe_file
fi

bazel --version
