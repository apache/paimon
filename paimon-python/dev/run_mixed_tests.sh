#!/bin/bash

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

# Mixed Java and Python test runner
# This script runs Java test first, then Python test to verify interoperability

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PAIMON_PYTHON_DIR="$PROJECT_ROOT/paimon-python/pypaimon/tests/e2e"
PAIMON_CORE_DIR="$PROJECT_ROOT/paimon-core"

echo -e "${YELLOW}=== Mixed Java-Python Read Write Test Runner ===${NC}"
echo "Project root: $PROJECT_ROOT"
echo "Paimon Python dir: $PAIMON_PYTHON_DIR"
echo "Paimon Core dir: $PAIMON_CORE_DIR"
echo ""

# Function to clean up warehouse directory
cleanup_warehouse() {
    echo -e "${YELLOW}=== Cleaning up warehouse directory ===${NC}"

    local warehouse_dir="$PAIMON_PYTHON_DIR/warehouse"

    if [[ -d "$warehouse_dir" ]]; then
        echo "Removing warehouse directory: $warehouse_dir"
        rm -rf "$warehouse_dir"
        echo -e "${GREEN}✓ Warehouse directory cleaned up successfully${NC}"
    else
        echo "Warehouse directory does not exist, no cleanup needed"
    fi

    echo ""
}

# Function to run Java test
run_java_write_test() {
    echo -e "${YELLOW}=== Step 1: Running Java Test (JavaPyE2ETest.testJavaWriteRead) ===${NC}"

    cd "$PROJECT_ROOT"

    # Run the specific Java test method
    echo "Running Maven test for JavaPyE2ETest.testJavaWriteRead..."
    if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testJavaWriteRead -pl paimon-core -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Java test failed${NC}"
        return 1
    fi
}

# Function to run Python test
run_python_read_test() {
    echo -e "${YELLOW}=== Step 2: Running Python Test (JavaPyReadWriteTest.testRead) ===${NC}"

    cd "$PAIMON_PYTHON_DIR"

    # Run the specific Python test method
    echo "Running Python test for JavaPyReadWriteTest.testRead..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read -v; then
        echo -e "${GREEN}✓ Python test completed successfully${NC}"
#        source deactivate
        return 0
    else
        echo -e "${RED}✗ Python test failed${NC}"
#        source deactivate
        return 1
    fi
}

# Function to run Python Write test for Python-Write-Java-Read scenario
run_python_write_test() {
    echo -e "${YELLOW}=== Step 3: Running Python Write Test (JavaPyReadWriteTest.test_py_write_read) ===${NC}"

    cd "$PAIMON_PYTHON_DIR"

    # Run the specific Python test method for writing data
    echo "Running Python test for JavaPyReadWriteTest.test_py_write_read (Python Write)..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_py_write_read -v; then
        echo -e "${GREEN}✓ Python write test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python write test failed${NC}"
        return 1
    fi
}

# Function to run Java Read test for Python-Write-Java-Read scenario
run_java_read_test() {
    echo -e "${YELLOW}=== Step 4: Running Java Read Test (JavaPyE2ETest.testRead) ===${NC}"

    cd "$PROJECT_ROOT"

    # Run the specific Java test method for reading Python-written data
    echo "Running Maven test for JavaPyE2ETest.testRead (Java Read)..."
    if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testRead -pl paimon-core -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java read test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Java read test failed${NC}"
        return 1
    fi
}

# Main execution
main() {
    local java_write_result=0
    local python_read_result=0
    local python_write_result=0
    local java_read_result=0

    echo -e "${YELLOW}Starting mixed language test execution...${NC}"
    echo ""

    # Run Java write test
    if ! run_java_write_test; then
        java_write_result=1
        echo -e "${RED}Java test failed, but continuing with Python test...${NC}"
        echo ""
    else
        echo ""
    fi

    # Run Python read test
    if ! run_python_read_test; then
        python_read_result=1
    fi

    echo ""

    # Run Python Write - Java Read test sequence
    echo -e "${YELLOW}Starting Python Write - Java Read test sequence...${NC}"
    echo ""

    # Run Python write test
    if ! run_python_write_test; then
        python_write_result=1
        echo -e "${RED}Python write test failed, but continuing with Java read test...${NC}"
        echo ""
    else
        echo ""
    fi

    # Run Java read test
    if ! run_java_read_test; then
        java_read_result=1
    fi

    echo ""
    echo -e "${YELLOW}=== Test Results Summary ===${NC}"

    if [[ $java_write_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Java Write Test (JavaPyE2ETest.testJavaWriteRead): PASSED${NC}"
    else
        echo -e "${RED}✗ Java Write Test (JavaPyE2ETest.testJavaWriteRead): FAILED${NC}"
    fi

    if [[ $python_read_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Python Read Test (JavaPyReadWriteTest.testRead): PASSED${NC}"
    else
        echo -e "${RED}✗ Python Read Test (JavaPyReadWriteTest.testRead): FAILED${NC}"
    fi

    if [[ $python_write_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Python Write Test (JavaPyReadWriteTest.test_py_write_read): PASSED${NC}"
    else
        echo -e "${RED}✗ Python Write Test (JavaPyReadWriteTest.test_py_write_read): FAILED${NC}"
    fi

    if [[ $java_read_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Java Read Test (JavaPyE2ETest.testRead): PASSED${NC}"
    else
        echo -e "${RED}✗ Java Read Test (JavaPyE2ETest.testRead): FAILED${NC}"
    fi

    echo ""

    # Clean up warehouse directory after all tests
    cleanup_warehouse

    if [[ $java_write_result -eq 0 && $python_read_result -eq 0 && $python_write_result -eq 0 && $java_read_result -eq 0 ]]; then
        echo -e "${GREEN}🎉 All tests passed! Java-Python interoperability verified.${NC}"
        return 0
    else
        echo -e "${RED}❌ Some tests failed. Please check the output above.${NC}"
        return 1
    fi
}

# Run main function
main "$@"