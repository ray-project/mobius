#!/bin/bash
script_dir=$(cd "$(dirname "${BASH_SOURCE:-$0}")"; pwd)

function pre_action()
{
  echo "Installation started."

  # install bazel
  if ! type "bazel" > /dev/null; then
    echo "Install bazel..."
    cd $script_dir
    sh ./install-bazel.sh
  else
    echo "Skip bazel installation."
  fi
}

function install_streaming()
{
  echo "Install streaming..."
  # install c++ and java
  cd $script_dir/../streaming && bazel build java:streaming_java_pkg
  cd $script_dir/../streamin/java && mvn clean install

  # install python
  cd $script_dir/../streaming/python && python setup.py install
}

function install_training()
{
  echo "Install training..."
}

function install_all()
{
    install_streaming
    install_training
}

function post_action
{
  echo "Installation completed."
}

# script started
if [[ -z $1 ]]; then pre_action && install_all
elif [[ $1 = 'skip_pre' ]]; then install_all
else
  case $1 in
      all)
          pre_action
          install_all
      ;;
      streaming)
          pre_action
          install_streaming
      ;;
      training)
          pre_action
          install_training
      ;;
  esac
fi

post_action
