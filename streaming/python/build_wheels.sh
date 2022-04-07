#!/bin/bash
script_dir=$(cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)

# Assuming in docker "quay.io/pypa/manylinux2014_x86_64".

build_with_python_version() {
pushd $script_dir
PYTHON_VERSION=$1
echo "Build Python Version ${PYTHON_BIN_PATH}"
export PYTHON_BIN_PATH="/opt/python/${PYTHON_VERSION}/bin/python"
PYTHON_BIN_PATH='/opt/python/${PYTHON_VERSION}/bin/python' /opt/python/${PYTHON_VERSION}/bin/python setup.py bdist_wheel
auditwheel repair --plat manylinux2014_x86_64 dist/raystreaming-0.0.1-${PYTHON_VERSION}-linux_x86_64.whl
bazel clean --expunge
/opt/python/$PYTHON_VERSION/bin/python setup.py clean --all
pushd
}

build_with_python_version cp38-cp38
#build_with_python_verion cp36-cp36m
#build_with_python_verion cp37-cp37m
