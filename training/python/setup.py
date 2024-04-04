import os

from setuptools import setup, find_packages

# the module name
pkg_dir = "streaming"

current_dir = os.path.dirname(os.path.abspath(__file__))

os.chdir(current_dir)

setup(
    name=pkg_dir,
    version="0.0.1",
    description="streaming",
    keywords=("ray", "streaming", "runtime", "operator"),
    author="The Authors of Antgroup",
    packages=find_packages(exclude=["*.tests", "*.tests.*", "tests.*", "tests"]),
    platforms="any",
    scripts=[],
    include_package_data=True,
    install_requires=["protobuf", "schedule", "psutil"],
    zip_safe=False,
)
