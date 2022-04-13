#!/bin/bash
script_dir=$(cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)

# Assuming in docker "quay.io/pypa/manylinux2014_x86_64".

build_with_python_version() {
pushd $script_dir
PYTHON_VERSION=$1
PYTHON3_HOME="/opt/python/${PYTHON_VERSION}"
PYTHON3_BIN_PATH="/opt/python/${PYTHON_VERSION}/bin/python"
echo "Build Python Version Path ${PYTHON3_HOME}"
if [ -d $PYTHON_VERSION ] ; then
  echo "Reuse python version virtualenv"
else 
  $PYTHON3_BIN_PATH -m pip install virtualenv
  $PYTHON3_BIN_PATH -m virtualenv -p $PYTHON3_BIN_PATH $script_dir/$PYTHON_VERSION
fi
. $script_dir/$PYTHON_VERSION/bin/activate

bash $script_dir/../../scripts/install-ray.sh
python setup.py bdist_wheel
auditwheel repair --plat manylinux_2_24_x86_64 dist/raystreaming-0.0.1-${PYTHON_VERSION}-*.whl
#bazel clean --expunge
/opt/python/$PYTHON_VERSION/bin/python setup.py clean --all
pushd
}

build_with_python_version cp38-cp38
build_with_python_version cp37-cp37m
build_with_python_version cp36-cp36m
