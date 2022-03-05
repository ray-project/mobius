#!/bin/bash
cd "$(dirname "${BASH_SOURCE:-$0}")" || exit

# Compare against the master branch, because most development is done against it.
base_commit="$(git merge-base HEAD master)"
if [ "$base_commit" = "$(git rev-parse HEAD)" ]; then
  # Prefix of master branch, so compare against parent commit
  base_commit="$(git rev-parse HEAD^)"
  echo "Running format against parent commit $base_commit"
else
  echo "Running format against parent commit $base_commit from master branch"
fi

exclude_regex="(.*thirdparty/)"
output="$(sh format.sh)"
if [ "$output" = "no modified files to format" ] || [ "$output" = "clang-format did not modify any files" ] ; then
  echo "format passed."
  exit 0
else
  echo "format failed:"
  echo "$output"
  exit 1
fi
