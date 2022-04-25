#!/bin/bash
current_dir=$(dirname "${BASH_SOURCE:-$0}")

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

# usage: upload [file] [target oss directory]
upload() {
  ossutil64 -i "${OSS_ID:-default-id}" -e "${OSS_HOST:-default-host}" -k "${OSS_KEY:-default-key}" cp "$1" oss://"${OSS_BUCKET:-default-bucket}${2}" -r -f
} 

# usage: zip_log_and_upload [directory] [fileName] [target oss directory]
zip_log_and_upload() {
  pushd "$current_dir" || exit

  echo "Zip log directory: ${1} into file: ${2}."
  zip -q -r "${2}" "${1}"

  echo "Upload file: ${2} to OSS: /ci/logs${3}."
  upload "${2} /ci/logs${3}"
}

publish_python () {
  pushd "$current_dir" || exit
  PYTHON_DIST_DIR=../streaming/python/dist
  echo "Head Commit ID :${GITHUB_SHA}"
  if [ -d $PYTHON_DIST_DIR ] ; then
    upload $PYTHON_DIST_DIR /publish/python/"$GITHUB_SHA"
  else
    echo "Python dist not found"
  fi
  popd || exit
}

if [ "$1" == "install" ] ; then
  install "$2"
elif [ "$1" == "cp" ] ; then
  upload "$2" "$3"
elif [ "$1" == "zip_log_and_upload" ] ; then
  zip_log_and_upload "$2" "$3" "$4"
elif [ "$1" == "publish_python" ] ; then
  publish_python
fi
