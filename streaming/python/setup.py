import os
import logging
import subprocess
from setuptools import setup, find_packages

logger = logging.getLogger(__name__)
file_path = os.getcwd()

# Build streaming package including java pkg by default
cwd = f"{file_path}/.."
bazel_build_cmd = [
    "bazel build //java:streaming_java_pkg",
    "bazel build streaming_pkg",
]

SUFFIX_ENV = ""
if "PYTHON_BIN_PATH" in os.environ:
    SUFFIX_ENV += "--python_path={}".format(os.environ["PYTHON_BIN_PATH"])

update_bazel_build_cmd = [" ".join([x, SUFFIX_ENV]) for x in bazel_build_cmd]
print("Update bazel build cmd {}".format(update_bazel_build_cmd))
for cmd in update_bazel_build_cmd:
    process = subprocess.Popen(cmd, cwd=cwd, shell=True)
    process.wait()

# Package raystreaming
setup(
    name="raystreaming",
    version="0.0.1",
    author="Ray Streaming Team",
    description="streaming module",
    packages=find_packages(),
    package_data={"raystreaming": ["_streaming.so", "jars/raystreaming_dist.jar"]},
    install_requires=["msgpack>=0.6.2", "pytest"],
)
