#!/bin/bash

cd "$(dirname "${BASH_SOURCE:-$0}")" || exit
ROOT="$(git rev-parse --show-toplevel)"

CLANG_FORMAT_VERSION_REQUIRED="12.0.0"
CLANG_FORMAT_VERSION=""
BLACK_VERSION_REQUIRED="21.12b0"
BLACK_VERSION=""
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

version_caution() {
    if [ "$2" != "$3" ]; then
        echo "WARNING: Require $1 in $3, You currently are using $2. This might generate different results."
    fi
}

check_format_command_exist()
{
  # check clang-format (c++)
  if command -v clang-format >/dev/null; then
    CLANG_FORMAT_VERSION=$(clang-format --version | awk '{print $3}')
    version_caution "clang-format" "$CLANG_FORMAT_VERSION" "$CLANG_FORMAT_VERSION_REQUIRED"
  else
      echo "'clang-format' is not installed. Install the clang-format=$CLANG_FORMAT_VERSION_REQUIRED with your system package manager."
      exit 1
  fi

  # check black (python)
  if command -v black >/dev/null; then
      BLACK_VERSION=$(black --version | awk '{print $3}')
      version_caution "black" "$BLACK_VERSION" "$BLACK_VERSION_REQUIRED"
  else
      echo "'black' is not installed. Install the python package with: pip install black==$BLACK_VERSION_REQUIRED"
      exit 1
  fi

  # check shellcheck (shell)
  if command -v shellcheck >/dev/null; then
      SHELLCHECK_VERSION=$(shellcheck --version | awk '/^version:/ {print $2}')
      version_caution "shellcheck" "$SHELLCHECK_VERSION" "$SHELLCHECK_VERSION_REQUIRED"
  else
      echo "'shellcheck' is not installed. Install the shellcheck=$SHELLCHECK_VERSION_REQUIRED with your system package manager."
      exit 1
  fi
}


# Format all files.
format_all()
{
    # go to root dir
    cd "${ROOT}" || exit 1

    echo "check shell using shellcheck..."
    shell_files=($(git ls-files -- '*.sh'))
    if [ 0 -lt "${#shell_files[@]}" ]; then
        shellcheck "${SHELLCHECK_EXCLUDES[@]}" "${shell_files[@]}"
    fi

    echo "format c++ using clang-format..."
    git ls-files -- '*.cc' '*.h' '*.proto' "${GIT_LS_EXCLUDES[@]}" | xargs -P 5 clang-format -i

    echo "format python using black..."
    git ls-files -- '*.py' "${GIT_LS_EXCLUDES[@]}" | xargs -P 10 black "${BLACK_EXCLUDES[@]}"

    echo "format java using 'maven-checkstyle-plugin'..."
    cd "${ROOT}"/streaming/java || exit 1
    mvn clean spotless:apply
}

# script started
check_format_command_exist
format_all
