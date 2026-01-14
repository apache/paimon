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
    echo -e "${YELLOW}=== Step 1: Running Java Write Tests (Parquet + Lance) ===${NC}"

    cd "$PROJECT_ROOT"

    # Run the Java test method for parquet format
    echo "Running Maven test for JavaPyE2ETest.testJavaWriteReadPkTable (Parquet)..."
    echo "Note: Maven may download dependencies on first run, this may take a while..."
    local parquet_result=0
    if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testJavaWriteReadPkTable -pl paimon-core -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java write parquet test completed successfully${NC}"
    else
        echo -e "${RED}✗ Java write parquet test failed${NC}"
        parquet_result=1
    fi

    echo ""

    # Run the Java test method for lance format
    echo "Running Maven test for JavaPyLanceE2ETest.testJavaWriteReadPkTableLance (Lance)..."
    echo "Note: Maven may download dependencies on first run, this may take a while..."
    local lance_result=0
    if mvn test -Dtest=org.apache.paimon.JavaPyLanceE2ETest#testJavaWriteReadPkTableLance -pl paimon-lance -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java write lance test completed successfully${NC}"
    else
        echo -e "${RED}✗ Java write lance test failed${NC}"
        lance_result=1
    fi

    if [[ $parquet_result -eq 0 && $lance_result -eq 0 ]]; then
        return 0
    else
        echo -e "${RED}✗ Java test failed${NC}"
        return 1
    fi
}

# Function to run Python test
run_python_read_test() {
    echo -e "${YELLOW}=== Step 2: Running Python Test (JavaPyReadWriteTest.test_read_pk_table) ===${NC}"

    cd "$PAIMON_PYTHON_DIR"

    # Run the parameterized Python test method (runs for both parquet and lance)
    echo "Running Python test for JavaPyReadWriteTest.test_read_pk_table..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest -k "test_read_pk_table" -v; then
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
    echo -e "${YELLOW}=== Step 3: Running Python Write Test (JavaPyReadWriteTest.test_py_write_read_pk_table) ===${NC}"

    cd "$PAIMON_PYTHON_DIR"

    # Run the parameterized Python test method for writing data (runs for both parquet and lance)
    echo "Running Python test for JavaPyReadWriteTest.test_py_write_read_pk_table (Python Write)..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest -k "test_py_write_read_pk_table" -v; then
        echo -e "${GREEN}✓ Python write test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python write test failed${NC}"
        return 1
    fi
}

# Function to run Java Read test for Python-Write-Java-Read scenario
run_java_read_test() {
    echo -e "${YELLOW}=== Step 4: Running Java Read Test (JavaPyE2ETest.testReadPkTable for parquet, JavaPyLanceE2ETest.testReadPkTableLance for lance) ===${NC}"

    cd "$PROJECT_ROOT"

    PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo "unknown")
    echo "Detected Python version: $PYTHON_VERSION"

    # Run Java test for parquet format in paimon-core
    echo "Running Maven test for JavaPyE2ETest.testReadPkTable (Java Read Parquet)..."
    echo "Note: Maven may download dependencies on first run, this may take a while..."
    local parquet_result=0
    if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testReadPkTable -pl paimon-core -Drun.e2e.tests=true -Dpython.version="$PYTHON_VERSION"; then
        echo -e "${GREEN}✓ Java read parquet test completed successfully${NC}"
    else
        echo -e "${RED}✗ Java read parquet test failed${NC}"
        parquet_result=1
    fi

    echo ""

    # Run Java test for lance format in paimon-lance
    echo "Running Maven test for JavaPyLanceE2ETest.testReadPkTableLance (Java Read Lance)..."
    echo "Note: Maven may download dependencies on first run, this may take a while..."
    local lance_result=0
    if mvn test -Dtest=org.apache.paimon.JavaPyLanceE2ETest#testReadPkTableLance -pl paimon-lance -Drun.e2e.tests=true -Dpython.version="$PYTHON_VERSION"; then
        echo -e "${GREEN}✓ Java read lance test completed successfully${NC}"
    else
        echo -e "${RED}✗ Java read lance test failed${NC}"
        lance_result=1
    fi

    if [[ $parquet_result -eq 0 && $lance_result -eq 0 ]]; then
        return 0
    else
        return 1
    fi
}
run_pk_dv_test() {
    echo -e "${YELLOW}=== Step 5: Running Primary Key & Deletion Vector Test (testPKDeletionVectorWriteRead) ===${NC}"

    cd "$PROJECT_ROOT"

    # Run the specific Java test method
    echo "Running Maven test for JavaPyE2ETest.testPKDeletionVectorWrite..."
    if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testPKDeletionVectorWrite -pl paimon-core -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java test completed successfully${NC}"
    else
        echo -e "${RED}✗ Java test failed${NC}"
        return 1
    fi
    cd "$PAIMON_PYTHON_DIR"
    # Run the specific Python test method
    echo "Running Python test for JavaPyReadWriteTest.test_pk_dv_read..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_pk_dv_read -v; then
        echo -e "${GREEN}✓ Python test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python test failed${NC}"
        return 1
    fi
}

# Function to run FAISS vector index test (Java write, Python read)
run_faiss_vector_test() {
    echo -e "${YELLOW}=== Step 6: Running FAISS Vector Index Test (Java Write, Python Read) ===${NC}"

    # Check Python version - skip FAISS tests for Python 3.6 as it has limited faiss-cpu support
    local python_version
    python_version=$(python -c "import sys; print(str(sys.version_info.major) + '.' + str(sys.version_info.minor))" 2>/dev/null || echo "unknown")
    if [[ "$python_version" == "3.6" ]]; then
        echo -e "${YELLOW}⊘ Skipping FAISS test for Python 3.6 (limited faiss-cpu support)${NC}"
        return 0
    fi

    cd "$PROJECT_ROOT"

    # Run the Java test method for FAISS vector index
    echo "Running Maven test for JavaPyFaissE2ETest.testJavaWriteFaissVectorIndex..."
    echo "Note: Maven may download dependencies on first run, this may take a while..."
    local mvn_output
    mvn_output=$(mvn test '-Dtest=org.apache.paimon.faiss.index.JavaPyFaissE2ETest#testJavaWriteFaissVectorIndex' -pl paimon-faiss/paimon-faiss-index -Drun.e2e.tests=true 2>&1)
    local mvn_exit_code=$?
    echo "$mvn_output"

    if [[ $mvn_exit_code -ne 0 ]]; then
        echo -e "${RED}✗ Java FAISS write test failed${NC}"
        return 1
    elif echo "$mvn_output" | grep -q "Tests run: 1.*Skipped: 1"; then
        # Test was skipped (FAISS native library not available)
        echo -e "${YELLOW}⊘ Java FAISS write test skipped (FAISS native library not available)${NC}"
        # Continue to Python test which will also skip
    else
        echo -e "${GREEN}✓ Java FAISS write test completed successfully${NC}"
    fi

    echo ""

    # Run the Python test method for reading FAISS vector table
    # Run from the tests directory where the test file is located
    cd "$PROJECT_ROOT/paimon-python/pypaimon/tests"
    echo "Running Python test for JavaPyFaissE2ETest.test_read_faiss_vector_table..."
    # Capture pytest output and check for "passed" since FAISS cleanup can cause segfault on exit
    local pytest_output
    pytest_output=$(python -m pytest test_global_index.py::JavaPyFaissE2ETest::test_read_faiss_vector_table -v --tb=short 2>&1) || true
    echo "$pytest_output"

    if echo "$pytest_output" | grep -qE "1 passed|passed.*1"; then
        echo -e "${GREEN}✓ Python FAISS read test completed successfully${NC}"
        return 0
    elif echo "$pytest_output" | grep -qE "1 skipped|skipped.*1"; then
        # Test was skipped (table not found, likely because Java test was also skipped)
        echo -e "${YELLOW}⊘ Python FAISS read test skipped (table not created - FAISS native library may not be available)${NC}"
        return 0
    else
        echo -e "${RED}✗ Python FAISS read test failed${NC}"
        return 1
    fi
}
# Main execution
main() {
    local java_write_result=0
    local python_read_result=0
    local python_write_result=0
    local java_read_result=0
    local pk_dv_result=0
    local faiss_vector_result=0

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

    # Run Java read test (handles both parquet and lance)
    if ! run_java_read_test; then
        java_read_result=1
    fi

    # Run pk dv read test
    if ! run_pk_dv_test; then
        pk_dv_result=1
    fi

    echo ""

    # Run FAISS vector index test (Java write, Python read)
    if ! run_faiss_vector_test; then
        faiss_vector_result=1
    fi

    echo ""
    echo -e "${YELLOW}=== Test Results Summary ===${NC}"

    if [[ $java_write_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Java Write Test (Parquet + Lance): PASSED${NC}"
    else
        echo -e "${RED}✗ Java Write Test (Parquet + Lance): FAILED${NC}"
    fi

    if [[ $python_read_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Python Read Test (JavaPyReadWriteTest.test_read_pk_table): PASSED${NC}"
    else
        echo -e "${RED}✗ Python Read Test (JavaPyReadWriteTest.test_read_pk_table): FAILED${NC}"
    fi

    if [[ $python_write_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Python Write Test (JavaPyReadWriteTest.test_py_write_read_pk_table): PASSED${NC}"
    else
        echo -e "${RED}✗ Python Write Test (JavaPyReadWriteTest.test_py_write_read_pk_table): FAILED${NC}"
    fi

    if [[ $java_read_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Java Read Test (Parquet + Lance): PASSED${NC}"
    else
        echo -e "${RED}✗ Java Read Test (Parquet + Lance): FAILED${NC}"
    fi

    if [[ $pk_dv_result -eq 0 ]]; then
        echo -e "${GREEN}✓ PK DV Test (JavaPyReadWriteTest.testPKDeletionVectorWriteRead): PASSED${NC}"
    else
        echo -e "${RED}✗ PK DV Test (JavaPyReadWriteTest.testPKDeletionVectorWriteRead): FAILED${NC}"
    fi

    if [[ $faiss_vector_result -eq 0 ]]; then
        echo -e "${GREEN}✓ FAISS Vector Index Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ FAISS Vector Index Test (Java Write, Python Read): FAILED${NC}"
    fi

    echo ""

    # Clean up warehouse directory after all tests
    cleanup_warehouse

    if [[ $java_write_result -eq 0 && $python_read_result -eq 0 && $python_write_result -eq 0 && $java_read_result -eq 0 && $pk_dv_result -eq 0 && $faiss_vector_result -eq 0 ]]; then
        echo -e "${GREEN}🎉 All tests passed! Java-Python interoperability verified.${NC}"
        return 0
    else
        echo -e "${RED}❌ Some tests failed. Please check the output above.${NC}"
        return 1
    fi
}

# Run main function
main "$@"