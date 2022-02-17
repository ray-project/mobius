import os
import logging
import subprocess
from setuptools import setup, find_packages

logger = logging.getLogger(__name__)
file_path = os.getcwd()

# Build streaming package including java pkg by default
cwd = f"{file_path}/.."
bazel_build_cmd = [
    "bazel build streaming_pkg --jobs=4",
    "bazel build //java:streaming_java_pkg --jobs=4"
]
for cmd in bazel_build_cmd:
    process = subprocess.Popen(bazel_build_cmd, cwd=cwd, shell=True)
    process.wait()

# Package raystreaming
setup(
    name="raystreaming",
    version="1.0",
    author="Ray Streaming Team",
    description="streaming module",
    packages=find_packages(),
    package_data={
        "raystreaming": ["_streaming.so", "jars/raystreaming_dist.jar"]
    },
    install_requires=["msgpack>=0.6.2"])
