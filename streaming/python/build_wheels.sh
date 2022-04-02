#!/bin/bash
script_dir=$(cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)

# Assuming in docker "quay.io/pypa/manylinux2014_x86_64".
pushd $script_dir
# Build py36 manylinux wheels
PYTHON_BIN_PATH='/opt/python/cp36-cp36m/bin/python' /opt/python/cp36-cp36m/bin/python setup.py bdist_wheel
auditwheel repair --plat manylinux2014_x86_64 dist/raystreaming-0.0.1-cp36-cp36m-linux_x86_64.whl
bazel clean --expunge
/opt/python/cp36-cp36m/bin/python setup.py clean --all

# Build py37 manylinux wheels
PYTHON_BIN_PATH='/opt/python/cp37-cp37m/bin/python' /opt/python/cp37-cp37m/bin/python setup.py bdist_wheel
auditwheel repair --plat manylinux2014_x86_64 dist/raystreaming-0.0.1-cp37-cp37m-linux_x86_64.whl
bazel clean --expunge
/opt/python/cp37-cp37m/bin/python setup.py clean --all

# Build py38 manylinux wheels
PYTHON_BIN_PATH='/opt/python/cp38-cp38/bin/python' /opt/python/cp38-cp38/bin/python setup.py bdist_wheel
auditwheel repair --plat manylinux2014_x86_64 dist/raystreaming-0.0.1-cp38-cp38-linux_x86_64.whl
bazel clean --expunge
/opt/python/cp38-cp38/bin/python setup.py clean --all

pushd
