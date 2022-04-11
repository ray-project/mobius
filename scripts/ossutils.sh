#!/bin/bash
current_dir=$(dirname "${BASH_SOURCE:-$0}")

install() {
oss_path=${1:-/usr/bin/}
wget "https://ray-mobius-us.oss-us-west-1.aliyuncs.com/ci/linux/ossutil64" -O $oss_path/ossutil64
chmod a+x $oss_path/ossutil64
}

copy() {
ossutil64 -i ${OSS_ID:-default-id} -e ${OSS_HOST:-default-host} -k ${OSS_KEY:-default-key} cp $1 oss://${OSS_BUCKET:-default-bucket}${2} -r -f
} 

publish_python () {
pushd $current_dir
PYTHON_DIST_DIR=../streaming/python/dist
COMMIT_ID=`git rev-parse HEAD`
echo "Head Commit ID :${COMMIT_ID}"
if [ -d $PYTHON_DIST_DIR ] ; then
	copy $PYTHON_DIST_DIR /publish/python/$COMMIT_ID
else
	echo "Python dist not found"
fi
popd
}

if [ $1 == "install" ] ; then
	install
elif [ $1 == "cp" ] ; then
	copy $2 $3
elif [ $1 == "publish_python" ] ; then
	publish_python
fi
