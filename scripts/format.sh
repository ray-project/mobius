#!/bin/bash

CURRENT_DIR=$(dirname "${BASH_SOURCE:-$0}")
cd $CURRENT_DIR || exit
ROOT="$(git rev-parse --show-toplevel)"

CLANG_FORMAT_VERSION_REQUIRED="12.0.0"
CLANG_FORMAT_VERSION=""
BLACK_VERSION_REQUIRED="21.12b0"
BLACK_VERSION=""
MAVEN_VERSION=""
SHELLCHECK_VERSION_REQUIRED="0.7.1"
SHELLCHECK_VERSION=""

BLACK_EXCLUDES=(
  #'--extend-exclude' 'python/xxx'
)
SHELLCHECK_EXCLUDES=(
  --exclude=1090  # "Can't follow non-constant source. Use a directive to specify location."
  --exclude=1091  # "Not following {file} due to some error"
  --exclude=2207  # "Prefer mapfile or read -a to split command output (or quote to avoid splitting)." -- these aren't compatible with macOS's old Bash
)
GIT_LS_EXCLUDES=(
  #':(exclude)python/xxx'
)
SKIP_BAZEL=0
SKIP_CPP=0
SKIP_JAVA=0
SKIP_PYTHON=0
SKIP_SHELL=0

usage()
{
  echo "Usage: format.sh [<args>]"
  echo
  echo "Options:"
  echo "  -h|--help                print the help info"
  echo "  -sb|--skip-bazel         skip bazel files format"
  echo "  -sc|--skip-cpp           skip cpp files format"
  echo "  -sj|--skip-java          skip java files format"
  echo "  -sp|--skip-python        skip python files format"
  echo "  -ss|--skip-shell         skip shell files format"
  echo
}

version_caution() {
    if [ "$2" != "$3" ]; then
        echo "WARNING: Require $1 in $3, You currently are using $2. This might generate different results."
    fi
}

check_format_command_exist()
{
  # check buildifier
  if command -v $CURRENT_DIR/buildifier >/dev/null; then
      BUILDIFIER_VERSION=$(${CURRENT_DIR}/buildifier --version | awk '{print $3}')
  else
      echo "'buildifier' is not installed. Use '-sb' to skip formatting bazel files or install the buildifier from
      'https://github.com/bazelbuild/buildtools/tree/master/buildifier'."
      exit 1
  fi

  # check clang-format (c++)
  if command -v clang-format >/dev/null; then
    CLANG_FORMAT_VERSION=$(clang-format --version | awk '{print $3}')
    version_caution "clang-format" "$CLANG_FORMAT_VERSION" "$CLANG_FORMAT_VERSION_REQUIRED"
  else
      echo "'clang-format' is not installed. Use '-sc' to skip formatting cpp files or install the
      clang-format=$CLANG_FORMAT_VERSION_REQUIRED with your system package manager."
      exit 1
  fi

  # check black (python)
  if command -v black >/dev/null; then
      BLACK_VERSION=$(black --version | awk '{print $3}')
      version_caution "black" "$BLACK_VERSION" "$BLACK_VERSION_REQUIRED"
  else
      echo "'black' is not installed. Use '-sp' to skip formatting python files or install the python package
      with: pip install black==$BLACK_VERSION_REQUIRED"
      exit 1
  fi

  # check maven (java)
    if command -v mvn >/dev/null; then
        MAVEN_VERSION=$(mvn --version |grep 'Apache Maven' |awk '{print $3}')
        echo "maven version: ${MAVEN_VERSION}"
    else
        echo "'maven' is not installed. Use '-sj' to skip formatting java files or install the maven(3.4+)
        with your system package manager."
        exit 1
    fi

  # check shellcheck (shell)
  if command -v shellcheck >/dev/null; then
      SHELLCHECK_VERSION=$(shellcheck --version | awk '/^version:/ {print $2}')
      version_caution "shellcheck" "$SHELLCHECK_VERSION" "$SHELLCHECK_VERSION_REQUIRED"
  else
      echo "'shellcheck' is not installed. Use '-ss' to skip formatting shells or install the
      shellcheck=$SHELLCHECK_VERSION_REQUIRED with your system package manager."
      exit 1
  fi
}


# Format all files.
format_all()
{
    # go to root dir
    cd "${ROOT}" || exit 1

    if [ $SKIP_BAZEL == 0 ]; then
      echo "=================== format bazel files using buildifier ==================="
      bazel_files="$(git ls-files -- '*.BUILD' '*.bazel' '*.bzl' 'BUILD' 'WORKSPACE')"
      if [ 0 -lt "${#bazel_files[@]}" ]; then
          buildifier -mode=fix -diff_command="diff -u" "${bazel_files[@]}"
      fi
    else
      echo "=================== skip formatting bazel files ==================="
    fi

    if [ $SKIP_SHELL == 0 ]; then
      echo "=================== check shell using shellcheck ==================="
      shell_files="$(git ls-files -- '*.sh')"
      if [ 0 -lt "${#shell_files[@]}" ]; then
          shellcheck "${SHELLCHECK_EXCLUDES[@]}" "${shell_files[@]}"
      fi
    else
      echo "=================== skip formatting shells ==================="
    fi

    if [ $SKIP_CPP == 0 ]; then
      echo "=================== format c++ using clang-format ==================="
      git ls-files -- '*.cc' '*.h' '*.proto' "${GIT_LS_EXCLUDES[@]}" | xargs -P 5 clang-format -i
    else
      echo "=================== skip formatting cpp files ==================="
    fi

    if [ $SKIP_PYTHON == 0 ]; then
      echo "=================== format python using black ==================="
      git ls-files -- '*.py' "${GIT_LS_EXCLUDES[@]}" | xargs -P 10 black "${BLACK_EXCLUDES[@]}"
    else
      echo "=================== skip formatting python files ==================="
    fi

    if [ $SKIP_JAVA == 0 ]; then
      echo "=================== format java using 'maven-checkstyle-plugin' ==================="
      cd "${ROOT}"/streaming/java || exit 1
      mvn clean spotless:apply
    else
      echo "=================== skip formatting java files ==================="
    fi
}

# Skip java dy default.
SKIP_JAVA=1
# script started
while [ $# -gt 0 ]; do
  key="$1"
  case $key in
    -h|--help)
      usage
      exit 0
      ;;
    -sb|--skip-bazel)
      SKIP_BAZEL=1
      ;;
    -sc|--skip-cpp)
      SKIP_CPP=1
      ;;
    -sj|--skip-java)
      SKIP_JAVA=1
      ;;
    -sp|--skip-python)
      SKIP_PYTHON=1
      ;;
    -ss|--skip-shell)
      SKIP_SHELL=1
      ;;
    *)
      echo "ERROR: unknown option \"$key\""
      echo
      usage
      exit 1
      ;;
  esac
  shift
done
check_format_command_exist
format_all
