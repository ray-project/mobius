#!/bin/bash
current_dir=$(dirname "${BASH_SOURCE:-$0}")

# shellcheck disable=SC2120
install() {
  oss_path="unknown"
  platform="unknown"

  case "${OSTYPE}" in
    msys)
      echo "Platform is Windows."
      platform="windows"
      # No installer for Windows
      ;;
    darwin*)
      echo "Platform is Mac OS X."
      platform="darwin"
      ;;
    linux*)
      echo "Platform is Linux (or WSL)."
      platform="linux"
      ;;
    *)
      echo "Platform is Linux (or WSL)."
      platform="linux"
      ;;
  esac
  echo "platform is ${platform}"
  
  if [ "${platform}" = "darwin" ]; then
      oss_path=${1:-/usr/local/bin}
      wget "https://ray-mobius-us.oss-us-west-1.aliyuncs.com/ci/macos/ossutilmac64" -O "$oss_path"/ossutil64
  else
      oss_path=${1:-/usr/bin}
      wget "https://ray-mobius-us.oss-us-west-1.aliyuncs.com/ci/linux/ossutil64" -O "$oss_path"/ossutil64
  fi
  
  chmod a+x "$oss_path"/ossutil64
  ossutil64 --version
}

upload() {
  ossutil64 -i "${OSS_ID:-default-id}" -e "${OSS_HOST:-default-host}" -k "${OSS_KEY:-default-key}" cp "$1" oss://"${OSS_BUCKET:-default-bucket}${2}" -r -f
} 

# usage: zip_dir_and_upload [directory] [fileName-prefix] [target oss directory]
zip_dir_and_upload() {
  pushd "$current_dir" || exit
  COMMIT_ID=$(git rev-parse HEAD)
  TIME=$(date '+%s')
  ZIP_FILE="${2}-${COMMIT_ID}-${TIME}.zip"

  echo "Zip directory: ${1} into file: ${ZIP_FILE}."
  zip -q -r "${ZIP_FILE}" "${1}"

  echo "Upload file: ${ZIP_FILE} to OSS: ${3}."
  upload "$ZIP_FILE $3"
}

publish_python () {
  pushd "$current_dir" || exit
  PYTHON_DIST_DIR=../streaming/python/dist
  COMMIT_ID=$(git rev-parse HEAD)
  echo "Head Commit ID :${COMMIT_ID}"
  if [ -d $PYTHON_DIST_DIR ] ; then
    upload $PYTHON_DIST_DIR /publish/python/$COMMIT_ID
  else
    echo "Python dist not found"
  fi
  popd || exit
}

if [ "$1" == "install" ] ; then
  install
elif [ "$1" == "cp" ] ; then
  upload "$2" "$3"
elif [ "$1" == "zip_dir_and_upload" ] ; then
  zip_dir_and_upload "$2" "$3" "$4"
elif [ "$1" == "publish_python" ] ; then
  publish_python
fi
