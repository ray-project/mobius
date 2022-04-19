#!/bin/bash
script_dir=$(cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)

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
ls dist/
auditwheel repair --plat manylinux_2_27_x86_64 dist/raystreaming-*.whl
exit_code=$?
if [ $exit_code != 0 ]; then
	echo "Exit code $exit_code"
	exit $exit_code
fi
#bazel clean --expunge
/opt/python/$PYTHON_VERSION/bin/python setup.py clean --all
pushd
}

build_with_python_version cp38-cp38
# Waiting for next ray released version.
#build_with_python_version cp37-cp37m
#build_with_python_version cp36-cp36m

# upload wheels to pypi
#twine upload dist/*.whl --verbose
