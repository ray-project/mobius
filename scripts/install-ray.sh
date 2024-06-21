#/bin/bash

minor_version=`python -c "import sys;print(sys.version_info[1])"`

python -m pip install -U ray
# if [ "$minor_version" == "8" ] ; then
# 	python -m pip install https://ray-mobius-us.oss-us-west-1.aliyuncs.com/ci/linux/ubuntu/b7d148815e427ccd47046d1c69ee3ab55bf7db3c/ray-2.0.0.dev0-cp38-cp38-linux_x86_64.whl
# elif [ "$minor_version" == "7" ] ; then
# 	python -m pip install https://ray-mobius-us.oss-us-west-1.aliyuncs.com/ci/linux/ubuntu/0bb82f29b65dca348acf5aa516d21ef3f176a3e1/ray-2.0.0.dev0-cp37-cp37m-linux_x86_64.whl
# elif [ "$minor_version" == "6"] ; then
# 	python -m pip install https://ray-mobius-us.oss-us-west-1.aliyuncs.com/ci/linux/ubuntu/0bb82f29b65dca348acf5aa516d21ef3f176a3e1/ray-2.0.0.dev0-cp36-cp36m-linux_x86_64.whl
# else
# 	echo "No such python version"
# fi
