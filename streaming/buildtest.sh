#!/bin/bash
script_dir=$(cd "$(dirname "${BASH_SOURCE:-$0}")" || exit; pwd)

function create_py_env()
{
    PY3_DIR=${1}
    python3 -m pip install virtualenv
    python3 -m virtualenv -p python3 $PY3_DIR
}

function init()
{
    pushd "$script_dir" || exit
    PY3_DIR=$script_dir/../py3
    if [ -d $PY3_DIR ]
    then
        echo "Reuse $PY3_DIR env"
    else
        create_py_env $PY3_DIR
    fi
    source $PY3_DIR/bin/activate
    echo "Source py3 env."
    popd || exit
}

# run a bunch of ut cases
# param 1 could be like examples below：
# raylet, java, python_core, python_non_core, streaming
# or combination of several param, splited by white space or comma, such as:
# raylet  java  python_core
# empty param means run all cases

function ut_all()
{
    run_case  "$@"
}

function compile()
{
    pushd "$script_dir" || exit
      bazel build //:streaming_lib
    popd || exit
}

function test_streaming_cpp() 
{
    pushd "$script_dir" || exit

    #bazel test //:all --test_filter=basic
    # NOTE(lingxuan.zlx): unsupported host instruction of bazel on github workflow
    bazel test "streaming_message_ring_buffer_tests" "barrier_helper_tests" "streaming_message_serialization_tests" "streaming_mock_transfer" \
    "streaming_util_tests" "streaming_perf_tests" "event_service_tests" "queue_protobuf_tests" "data_writer_tests" "buffer_pool_tests"
    exit $?

    popd || exit
}

function test_streaming_java() 
{
    pushd "$script_dir" || exit

    bazel build libstreaming_java.so
    pushd $script_dir/java
    sh test.sh
    exit $?

    popd || exit
}

function test_streaming_python() 
{
    pushd "$script_dir" || exit
    # Avoid macos build in python2
    if [[ $OSTYPE == "darwin" ]]; then
        pushd $script_dir/python
        python3 setup.py install --verbose
        popd
    else
        pip install -e python --verbose 
    fi
    #python3 -m pytest $script_dir/python/raystreaming/tests/simple --capture=no
    bazel build java:streaming_java_pkg
    python3 -m pytest $script_dir/python/raystreaming/tests/ --capture=no
    exit $?

    popd || exit
}



function streaming_package() 
{
    pushd "$script_dir" || exit

    bazel build streaming_pkg
    exit $?

    popd || exit
}

function run_case()
{
    local test_categories="$1"
    if [[ "$test_categories" == "" ]]; then
      test_categories="streaming_package streaming_cpp streaming_java streaming_python"
    fi

    cd $script_dir

    if [[ "$test_categories" == *package* ]]; then
      echo "Running package."
      set +e
      
      streaming_package
      CODE=$?

      if [[ $CODE != 0 ]]; then
        exit $CODE
      fi
    fi

    if [[ "$test_categories" == *java* ]]; then
      echo "Running java test cases."
      set +e
      
      test_streaming_java
      CODE=$?

      if [[ $CODE != 0 ]]; then
        exit $CODE
      fi
    fi

    if [[ "$test_categories" == *python* ]]; then
      echo "Running python test cases."
      set +e
      
      test_streaming_python
      CODE=$?

      if [[ $CODE != 0 ]]; then
        exit $CODE
      fi
    fi

    if [[ "$test_categories" == *cpp* ]]; then
      echo "Running cpp tests."
      set +e
      
      test_streaming_cpp
      CODE=$?

      if [[ $CODE != 0 ]]; then
        exit $CODE
      fi
    fi
}

function usage(){
  echo "use like this:"
  echo 'sh buildtest.sh  --test_categories="compile,streaming_cpp,streaming_java" --'
  echo 'sh buildtest.sh --"'
  echo '--test_categories: specify which type of tests you want to run'
  echo '                   you can specify one or several types'
  echo '                   splited by white space or comma'
}


TEST_CATEGORIES=""

optspec=":hv-:"
while getopts "$optspec" optchar; do
    case "${optchar}" in
        -)
            case "${OPTARG}" in
                test_categories)
                    val="${!OPTIND}"; OPTIND=$(( OPTIND + 1 ))
                    echo "Parsing option: '--${OPTARG}', value: '${val}'" >&2;
                    ;;
                test_categories=*)
                    val=${OPTARG#*=}
                    opt=${OPTARG%=$val}
                    echo "Parsing option: '--${opt}', value: '${val}'" >&2
                    TEST_CATEGORIES=${val}
                    ;;
                *)
                    if [ "$OPTERR" = 1 ] && [ "${optspec:0:1}" != ":" ]; then
                        echo "Unknown option --${OPTARG}" >&2
                    fi
                    ;;
            esac;;
        h)
            usage
            exit 2
            ;;
        *)
            if [ "$OPTERR" != 1 ] || [ "${optspec:0:1}" = ":" ]; then
                echo "Non-option argument: '-${OPTARG}'" >&2
            fi
            ;;
    esac
done

if [[ $TEST_CATEGORIES == "raylet" ]]; then
    shift $((OPTIND - 1))
    sanitizer=$1

    case ${sanitizer} in
        "asan")
            SANITIZER_PARAM="--config=asan";;
        "tsan")
            SANITIZER_PARAM="--config=tsan"
            cat /proc/sys/kernel/randomize_va_space
            ;;
        *)
            echo "no "${sanitizer}
    esac
fi

# To shorten compile time we disable compile before cpp test.
#compile

#if [[ "$TEST_CATEGORIES" != *lint* ]]; then
#  compile
#fi
init
ut_all $TEST_CATEGORIES
