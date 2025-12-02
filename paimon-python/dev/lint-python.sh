#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

# lint-python.sh
# This script will prepare a virtual environment for many kinds of checks, such as tox check, flake8 check.
#

# Printing infos both in log and console
function print_function() {
    local STAGE_LENGTH=48
    local left_edge_len=
    local right_edge_len=
    local str
    case "$1" in
        "STAGE")
            left_edge_len=$(((STAGE_LENGTH-${#2})/2))
            right_edge_len=$((STAGE_LENGTH-${#2}-left_edge_len))
            str="$(seq -s "=" $left_edge_len | tr -d "[:digit:]")""$2""$(seq -s "=" $right_edge_len | tr -d "[:digit:]")"
            ;;
        "STEP")
            str="$2"
            ;;
        *)
            str="seq -s "=" $STAGE_LENGTH | tr -d "[:digit:]""
            ;;
    esac
    echo $str | tee -a $LOG_FILE
}

function regexp_match() {
    if echo $1 | grep -e $2 &>/dev/null; then
        echo true
    else
        echo false
    fi
}

# decide whether a array contains a specified element.
function contains_element() {
    arr=($1)
    if echo "${arr[@]}" | grep -w "$2" &>/dev/null; then
        echo true
    else
        echo false
    fi
}

# create dir if needed
function create_dir() {
    if [ ! -d $1 ]; then
        mkdir -p $1
        if [ $? -ne 0 ]; then
            echo "mkdir -p $1 failed. you can mkdir manually or exec the script with \
            the command: sudo ./lint-python.sh"
            exit 1
        fi
    fi
}

# Collect checks
function collect_checks() {
    if [ ! -z "$EXCLUDE_CHECKS" ] && [ ! -z  "$INCLUDE_CHECKS" ]; then
        echo "You can't use option -s and -e simultaneously."
        exit 1
    fi
    if [ ! -z "$EXCLUDE_CHECKS" ]; then
        for (( i = 0; i < ${#EXCLUDE_CHECKS[@]}; i++)); do
            if [[ `contains_element "${SUPPORT_CHECKS[*]}" "${EXCLUDE_CHECKS[i]}_check"` = true ]]; then
                SUPPORT_CHECKS=("${SUPPORT_CHECKS[@]/${EXCLUDE_CHECKS[i]}_check}")
            else
                echo "the check ${EXCLUDE_CHECKS[i]} is invalid."
                exit 1
            fi
        done
    fi
    if [ ! -z "$INCLUDE_CHECKS" ]; then
        REAL_SUPPORT_CHECKS=()
        for (( i = 0; i < ${#INCLUDE_CHECKS[@]}; i++)); do
            if [[ `contains_element "${SUPPORT_CHECKS[*]}" "${INCLUDE_CHECKS[i]}_check"` = true ]]; then
                REAL_SUPPORT_CHECKS+=("${INCLUDE_CHECKS[i]}_check")
            else
                echo "the check ${INCLUDE_CHECKS[i]} is invalid."
                exit 1
            fi
        done
        SUPPORT_CHECKS=(${REAL_SUPPORT_CHECKS[@]})
    fi
}

# get all supported checks functions
function get_all_supported_checks() {
    _OLD_IFS=$IFS
    IFS=$'\n'
    SUPPORT_CHECKS=("flake8_check" "pytest_check" "mixed_check") # control the calling sequence
    for fun in $(declare -F); do
        if [[ `regexp_match "$fun" "_check$"` = true ]]; then
            check_name="${fun:11}"
            # Only add if not already in SUPPORT_CHECKS
            if [[ ! `contains_element "${SUPPORT_CHECKS[*]}" "$check_name"` = true ]]; then
                SUPPORT_CHECKS+=("$check_name")
            fi
        fi
    done
    IFS=$_OLD_IFS
}

# exec all selected check stages
function check_stage() {
    print_function "STAGE" "checks starting"
    for fun in "${SUPPORT_CHECKS[@]}"; do
        $fun
    done
    echo "All the checks are finished, the detailed information can be found in: $LOG_FILE"
}

###############################################################All Checks Definitions###############################################################
#########################
# This part defines all check functions such as tox_check and flake8_check
# We make a rule that all check functions are suffixed with _ check. e.g. tox_check, flake8_check
#########################
# Flake8 check
function flake8_check() {
    local PYTHON_SOURCE="$(find . \( -path ./dev -o -path ./.tox -o -path ./.venv \) -prune -o -type f -name "*.py" -print )"

    print_function "STAGE" "flake8 checks"
    if [ ! -f "$FLAKE8_PATH" ]; then
        echo "For some unknown reasons, the flake8 package is not complete."
    fi

    if [[ ! "$PYTHON_SOURCE" ]]; then
        echo "No python files found!  Something is wrong exiting."
        exit 1;
    fi

    # the return value of a pipeline is the status of the last command to exit
    # with a non-zero status or zero if no command exited with a non-zero status
    set -o pipefail
    ($FLAKE8_PATH  --config=./dev/cfg.ini $PYTHON_SOURCE) 2>&1 | tee -a $LOG_FILE

    PYCODESTYLE_STATUS=$?
    if [ $PYCODESTYLE_STATUS -ne 0 ]; then
        print_function "STAGE" "flake8 checks... [FAILED]"
        # Stop the running script.
        exit 1;
    else
        print_function "STAGE" "flake8 checks... [SUCCESS]"
    fi
}

# Pytest check
function pytest_check() {
    print_function "STAGE" "pytest checks"
    if [ ! -f "$PYTEST_PATH" ]; then
        echo "For some unknown reasons, the pytest package is not complete."
    fi

    # Get Python version
    PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    echo "Detected Python version: $PYTHON_VERSION"

    # Determine test directory based on Python version
    if [ "$PYTHON_VERSION" = "3.6" ]; then
        TEST_DIR="pypaimon/tests/py36"
        echo "Running tests for Python 3.6: $TEST_DIR"
    else
        TEST_DIR="pypaimon/tests --ignore=pypaimon/tests/py36 --ignore=pypaimon/tests/e2e"
        echo "Running tests for Python $PYTHON_VERSION (excluding py36): pypaimon/tests --ignore=pypaimon/tests/py36"
    fi

    # the return value of a pipeline is the status of the last command to exit
    # with a non-zero status or zero if no command exited with a non-zero status
    set -o pipefail
    ($PYTEST_PATH $TEST_DIR) 2>&1 | tee -a $LOG_FILE

    PYCODESTYLE_STATUS=$?
    if [ $PYCODESTYLE_STATUS -ne 0 ]; then
        print_function "STAGE" "pytest checks... [FAILED]"
        # Stop the running script.
        exit 1;
    else
        print_function "STAGE" "pytest checks... [SUCCESS]"
    fi
}

# Mixed tests check - runs Java-Python interoperability tests
function mixed_check() {
    # Get Python version
    PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    echo "Detected Python version: $PYTHON_VERSION"
    if [ "$PYTHON_VERSION" = "3.6" ]; then
        print_function "STAGE" "mixed tests checks... [SKIPPED]"
        return
    fi
    print_function "STAGE" "mixed tests checks"

    # Path to the mixed tests script
    MIXED_TESTS_SCRIPT="$CURRENT_DIR/dev/run_mixed_tests.sh"

    if [ ! -f "$MIXED_TESTS_SCRIPT" ]; then
        echo "Mixed tests script not found at: $MIXED_TESTS_SCRIPT"
        print_function "STAGE" "mixed tests checks... [FAILED]"
        exit 1
    fi

    # Make sure the script is executable
    chmod +x "$MIXED_TESTS_SCRIPT"

    # Run the mixed tests script
    set -o pipefail
    ($MIXED_TESTS_SCRIPT) 2>&1 | tee -a $LOG_FILE

    MIXED_TESTS_STATUS=$?
    if [ $MIXED_TESTS_STATUS -ne 0 ]; then
        print_function "STAGE" "mixed tests checks... [FAILED]"
        # Stop the running script.
        exit 1;
    else
        print_function "STAGE" "mixed tests checks... [SUCCESS]"
    fi
}
###############################################################All Checks Definitions###############################################################
# CURRENT_DIR is "paimon-python/"
SCRIPT_PATH="$(readlink -f "$0")"
cd "$(dirname "$SCRIPT_PATH")/.." || exit
CURRENT_DIR="$(pwd)"
echo ${CURRENT_DIR}

# flake8 path
FLAKE8_PATH="$(which flake8)"
# pytest path
PYTEST_PATH="$(which pytest)"
echo $PYTEST_PATH

LOG_DIR=$CURRENT_DIR/dev/log

if [ "$PAIMON_IDENT_STRING" == "" ]; then
    PAIMON_IDENT_STRING="$USER"
fi
if [ "$HOSTNAME" == "" ]; then
    HOSTNAME="$HOST"
fi

# the log file stores the checking result.
LOG_FILE=$LOG_DIR/paimon-$PAIMON_IDENT_STRING-python-$HOSTNAME.log
create_dir $LOG_DIR

# clean LOG_FILE content
echo >$LOG_FILE

SUPPORT_CHECKS=()

# search all supported check functions and put them into SUPPORT_CHECKS array
get_all_supported_checks

EXCLUDE_CHECKS=""

INCLUDE_CHECKS=""

# parse_opts
USAGE="
usage: $0 [options]
-h          print this help message and exit
-e [tox,flake8,sphinx,mypy,mixed]
            exclude checks which split by comma(,)
-i [tox,flake8,sphinx,mypy,mixed]
            include checks which split by comma(,)
-l          list all checks supported.
Examples:
  ./lint-python.sh                 =>  exec all checks.
  ./lint-python.sh -e tox,flake8   =>  exclude checks tox,flake8.
  ./lint-python.sh -i flake8       =>  include checks flake8.
  ./lint-python.sh -i mixed        =>  include checks mixed.
  ./lint-python.sh -l              =>  list all checks supported.
"
while getopts "hfs:i:e:lr" arg; do
    case "$arg" in
        h)
            printf "%s\\n" "$USAGE"
            exit 2
            ;;
        e)
            EXCLUDE_CHECKS=($(echo $OPTARG | tr ',' ' ' ))
            ;;
        i)
            INCLUDE_CHECKS=($(echo $OPTARG | tr ',' ' ' ))
            ;;
        l)
            printf "current supported checks includes:\n"
            for fun in "${SUPPORT_CHECKS[@]}"; do
                echo ${fun%%_check*}
            done
            exit 2
            ;;
        ?)
            printf "ERROR: did not recognize option '%s', please try -h\\n" "$1"
            exit 1
            ;;
    esac
done

# collect checks according to the options
collect_checks
# run checks
check_stage