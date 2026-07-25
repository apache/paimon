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

JAVA_WRITE_BATCH_DONE=false

java_write_batch_enabled() {
    [[ "${MIXED_TESTS_BATCH_JAVA_WRITES:-true}" != "false" ]]
}

skip_batched_java_write() {
    if [[ "${JAVA_WRITE_BATCH_DONE:-false}" == "true" ]]; then
        echo "Java write already completed in batched phase; skipping Maven call."
        return 0
    fi
    return 1
}

run_maven_test_batch() {
    local description="$1"
    local module="$2"
    local test_spec="$3"
    shift 3

    cd "$PROJECT_ROOT"
    echo "Running Maven batch for $description..."
    echo "  Module: $module"
    echo "  Tests: $test_spec"
    if mvn test -Dtest="$test_spec" -pl "$module" -Drun.e2e.tests=true "$@"; then
        echo -e "${GREEN}✓ $description completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ $description failed${NC}"
        return 1
    fi
}

run_batched_java_write_tests() {
    echo -e "${YELLOW}=== Pre-step: Running Batched Java Write Tests ===${NC}"
    echo "Set MIXED_TESTS_BATCH_JAVA_WRITES=false to use the original one-by-one Java write flow."

    local result=0

    local core_tests="org.apache.paimon.JavaPyE2ETest#testJavaWriteReadPkTable"
    core_tests="${core_tests}+testPKDeletionVectorWrite"
    core_tests="${core_tests}+testBtreeIndexWrite"
    core_tests="${core_tests}+testBtreeRawFallbackWrite"
    core_tests="${core_tests}+testBitmapIndexWrite"
    core_tests="${core_tests}+testCompressedGlobalIndexWrite"
    core_tests="${core_tests}+testJavaWriteCompressedTextAppendTable"
    core_tests="${core_tests}+testJavaWriteVectorAppendTable"
    core_tests="${core_tests}+testCompactConflictWriteBase"
    core_tests="${core_tests}+testBlobCompactConflictWriteBase"
    core_tests="${core_tests}+testBlobWriteAlterCompact"
    core_tests="${core_tests}+testJavaWriteArrayBlobTable"
    core_tests="${core_tests}+testJavaWriteMapBlobTable"
    core_tests="${core_tests}+testDataEvolutionWrite"
    core_tests="${core_tests}+testJavaWriteRowAppendTable"
    if [[ "$PYTHON_MINOR" -ge 7 ]]; then
        core_tests="${core_tests}+testJavaWriteVariantTable"
    fi
    if ! run_maven_test_batch "paimon-core Java write tests" "paimon-core" "$core_tests" -q; then
        result=1
    fi

    # lance has no Python wheel on <3.8; its readers are all skipped there.
    if [[ "$PYTHON_MINOR" -ge 8 ]]; then
        local lance_tests="org.apache.paimon.JavaPyLanceE2ETest#testJavaWriteReadPkTableLance"
        lance_tests="${lance_tests}+testDataEvolutionWriteLance"
        if ! run_maven_test_batch "paimon-lance Java write tests" "paimon-lance" "$lance_tests" -q; then
            result=1
        fi
    fi

    if [[ "$PYTHON_MINOR" -ge 11 ]]; then
        local vortex_tests="org.apache.paimon.JavaPyVortexE2ETest#testJavaWriteVectorDedicatedFile"
        vortex_tests="${vortex_tests}+testJavaWriteMultiVectorDedicatedFile"
        if ! run_maven_test_batch "paimon-vortex Java write tests" "paimon-vortex/paimon-vortex-format" "$vortex_tests" -q; then
            result=1
        fi
    fi

    if [[ "$PYTHON_MINOR" -ge 8 ]]; then
        if ! run_maven_test_batch \
            "paimon-full-text Java write tests" \
            "paimon-full-text" \
            "org.apache.paimon.fulltext.index.JavaPyNativeFullTextE2ETest#testNativeFullTextIndexWrite" \
            -q; then
            result=1
        fi
    fi

    # Lumina vector reads need BitMap64 (>=3.8); skip the write below it too.
    if [[ "$PYTHON_MINOR" -ge 8 ]]; then
        local lumina_tests="org.apache.paimon.lumina.index.JavaPyLuminaE2ETest#testLuminaVectorIndexWrite"
        lumina_tests="${lumina_tests}+testLuminaVectorWithBTreeIndexWrite"
        if ! run_maven_test_batch "paimon-lumina Java write tests" "paimon-lumina" "$lumina_tests" -q; then
            result=1
        fi
    fi

    if [[ "$PYTHON_MINOR" -ge 9 ]]; then
        local vindex_tests="org.apache.paimon.JavaPyE2ETest#testVindexVectorIndexWrite"
        vindex_tests="${vindex_tests}+testVindexVectorRawFallbackWrite"
        if ! run_maven_test_batch \
            "paimon-vector Java write tests" \
            "paimon-vector" \
            "$vindex_tests" \
            -am -q -DfailIfNoTests=false; then
            result=1
        fi
    fi

    echo ""
    return $result
}

# Function to run Java test
run_java_write_test() {
    echo -e "${YELLOW}=== Step 1: Running Java Write Tests (Parquet/Orc/Avro + Lance) ===${NC}"

    if skip_batched_java_write; then
        return 0
    fi

    cd "$PROJECT_ROOT"

    # Run the Java test method for Parquet/Orc/Avro format
    echo "Running Maven test for JavaPyE2ETest.testJavaWriteReadPkTable (Parquet/Orc/Avro)..."
    echo "Note: Maven may download dependencies on first run, this may take a while..."
    local parquet_result=0
    if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testJavaWriteReadPkTable -pl paimon-core -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java write Parquet/Orc/Avro test completed successfully${NC}"
    else
        echo -e "${RED}✗ Java write Parquet/Orc/Avro test failed${NC}"
        parquet_result=1
    fi

    echo ""

    # Run the Java test method for lance format (readers skipped on <3.8).
    local lance_result=0
    if [[ "$PYTHON_MINOR" -ge 8 ]]; then
        echo "Running Maven test for JavaPyLanceE2ETest.testJavaWriteReadPkTableLance (Lance)..."
        echo "Note: Maven may download dependencies on first run, this may take a while..."
        if mvn test -Dtest=org.apache.paimon.JavaPyLanceE2ETest#testJavaWriteReadPkTableLance -pl paimon-lance -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java write lance test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java write lance test failed${NC}"
            lance_result=1
        fi
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

    # Run the parameterized Python test method (runs for both Parquet/Orc/Avro and Lance)
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
    echo -e "${YELLOW}=== Step 3: Running Python Write Test (test_py_write_read_pk_table) ===${NC}"

    cd "$PAIMON_PYTHON_DIR"

    # Run the parameterized Python test method for writing data (pk table, includes bucket num assertion)
    echo "Running Python test for JavaPyReadWriteTest (test_py_write_read_pk_table)..."
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
    echo -e "${YELLOW}=== Step 4: Running Java Read Test (testReadPkTable, testReadPkTableLance) ===${NC}"

    cd "$PROJECT_ROOT"

    # Run Java test for Parquet/Orc/Avro format in paimon-core
    echo "Running Maven test for JavaPyE2ETest.testReadPkTable (Java Read Parquet/Orc/Avro)..."
    echo "Note: Maven may download dependencies on first run, this may take a while..."
    local parquet_result=0
    if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testReadPkTable -pl paimon-core -Drun.e2e.tests=true -Dpython.version="$PYTHON_VERSION"; then
        echo -e "${GREEN}✓ Java read Parquet/Orc/Avro test completed successfully${NC}"
    else
        echo -e "${RED}✗ Java read Parquet/Orc/Avro test failed${NC}"
        parquet_result=1
    fi

    echo ""

    # Java read Lance reads a Python-written table; Python skips lance on <3.8.
    local lance_result=0
    if [[ "$PYTHON_MINOR" -ge 8 ]]; then
        echo "Running Maven test for JavaPyLanceE2ETest.testReadPkTableLance (Java Read Lance)..."
        echo "Note: Maven may download dependencies on first run, this may take a while..."
        if mvn test -Dtest=org.apache.paimon.JavaPyLanceE2ETest#testReadPkTableLance -pl paimon-lance -Drun.e2e.tests=true -Dpython.version="$PYTHON_VERSION"; then
            echo -e "${GREEN}✓ Java read lance test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java read lance test failed${NC}"
            lance_result=1
        fi
    else
        echo -e "${YELLOW}⏭ Skipping Java read Lance (lance needs Python >= 3.8, current: $PYTHON_VERSION)${NC}"
    fi

    if [[ $parquet_result -eq 0 && $lance_result -eq 0 ]]; then
        return 0
    else
        return 1
    fi
}

run_pk_dv_test() {
    echo -e "${YELLOW}=== Step 5: Running Primary Key & Deletion Vector Test (testPKDeletionVectorWriteRead) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        # Run the specific Java test method
        echo "Running Maven test for JavaPyE2ETest.testPKDeletionVectorWrite..."
        if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testPKDeletionVectorWrite -pl paimon-core -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java test failed${NC}"
            return 1
        fi
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

# Function to run BTree index test (Java write, Python read)
run_btree_index_test() {
    echo -e "${YELLOW}=== Step 6: Running BTree Index Test (Java Write, Python Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyE2ETest.testBtreeIndexWrite..."
        if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testBtreeIndexWrite -pl paimon-core -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java test failed${NC}"
            return 1
        fi
    fi
    cd "$PAIMON_PYTHON_DIR"
    # Run the specific Python test method
    echo "Running Python test for JavaPyReadWriteTest.test_read_btree_index_table..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_btree_index_table -v; then
        echo -e "${GREEN}✓ Python test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python test failed${NC}"
        return 1
    fi
}

run_btree_raw_fallback_test() {
    echo -e "${YELLOW}=== Running BTree Raw Fallback Test (Java Write, Python Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyE2ETest.testBtreeRawFallbackWrite..."
        if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testBtreeRawFallbackWrite -pl paimon-core -am -q -DfailIfNoTests=false -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java test failed${NC}"
            return 1
        fi
    fi
    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_read_btree_raw_fallback..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_btree_raw_fallback -v; then
        echo -e "${GREEN}✓ Python test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python test failed${NC}"
        return 1
    fi
}

run_bitmap_index_test() {
    echo -e "${YELLOW}=== Step 6b: Running Bitmap Index Test (Java Write, Python Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyE2ETest.testBitmapIndexWrite..."
        if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testBitmapIndexWrite -pl paimon-core -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java test failed${NC}"
            return 1
        fi
    fi
    cd "$PAIMON_PYTHON_DIR"
    # Run the specific Python test method
    echo "Running Python test for JavaPyReadWriteTest.test_read_bitmap_index_table..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_bitmap_index_table -v; then
        echo -e "${GREEN}✓ Python test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python test failed${NC}"
        return 1
    fi
}

run_compressed_global_index_test() {
    echo -e "${YELLOW}=== Step 6c: Running Compressed Global Index Test (Java Write, Python Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyE2ETest.testCompressedGlobalIndexWrite..."
        if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testCompressedGlobalIndexWrite -pl paimon-core -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java test failed${NC}"
            return 1
        fi
    fi
    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_read_compressed_global_index_fallback_scan..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_compressed_global_index_fallback_scan -v; then
        echo -e "${GREEN}✓ Python test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python test failed${NC}"
        return 1
    fi
}

run_compressed_text_test() {
    echo -e "${YELLOW}=== Step 7: Running Compressed Text Test (Java Write, Python Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyE2ETest.testJavaWriteCompressedTextAppendTable..."
        if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testJavaWriteCompressedTextAppendTable -pl paimon-core -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java test failed${NC}"
            return 1
        fi
    fi
    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_read_compressed_text_append_table..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest -k "test_read_compressed_text_append_table" -v; then
        echo -e "${GREEN}✓ Python test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python test failed${NC}"
        return 1
    fi
}

run_vector_append_table_test() {
    echo -e "${YELLOW}=== Running Vector Append Table Test (Java Write, Python Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyE2ETest.testJavaWriteVectorAppendTable..."
        if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testJavaWriteVectorAppendTable -pl paimon-core -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java test failed${NC}"
            return 1
        fi
    fi
    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_read_vector_append_table..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_vector_append_table -v; then
        echo -e "${GREEN}✓ Python test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python test failed${NC}"
        return 1
    fi
}

run_vector_dedicated_file_java_write_test() {
    echo -e "${YELLOW}=== Running Vector Dedicated File Test (Java Write, Python Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyVortexE2ETest.testJavaWriteVectorDedicatedFile..."
        if mvn test -Dtest=org.apache.paimon.JavaPyVortexE2ETest#testJavaWriteVectorDedicatedFile -pl paimon-vortex/paimon-vortex-format -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java vector dedicated file write completed successfully${NC}"
        else
            echo -e "${RED}✗ Java vector dedicated file write failed${NC}"
            return 1
        fi
    fi
    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_read_vector_dedicated_file..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_vector_dedicated_file -v; then
        echo -e "${GREEN}✓ Python vector dedicated file read completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python vector dedicated file read failed${NC}"
        return 1
    fi
}

run_vector_dedicated_file_py_write_test() {
    echo -e "${YELLOW}=== Running Vector Dedicated File Test (Python Write, Java Read) ===${NC}"

    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_py_write_vector_dedicated_file..."
    if ! python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_py_write_vector_dedicated_file -v; then
        echo -e "${RED}✗ Python vector dedicated file write failed${NC}"
        return 1
    fi
    echo -e "${GREEN}✓ Python vector dedicated file write completed successfully${NC}"

    echo ""

    cd "$PROJECT_ROOT"
    echo "Running Maven test for JavaPyVortexE2ETest.testJavaReadVectorDedicatedFile..."
    if mvn test -Dtest=org.apache.paimon.JavaPyVortexE2ETest#testJavaReadVectorDedicatedFile -pl paimon-vortex/paimon-vortex-format -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java vector dedicated file read completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Java vector dedicated file read failed${NC}"
        return 1
    fi
}

run_multi_vector_dedicated_file_java_write_test() {
    echo -e "${YELLOW}=== Running Multi-Vector Dedicated File Test (Java Write, Python Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyVortexE2ETest.testJavaWriteMultiVectorDedicatedFile..."
        if mvn test -Dtest=org.apache.paimon.JavaPyVortexE2ETest#testJavaWriteMultiVectorDedicatedFile -pl paimon-vortex/paimon-vortex-format -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java multi-vector dedicated file write completed successfully${NC}"
        else
            echo -e "${RED}✗ Java multi-vector dedicated file write failed${NC}"
            return 1
        fi
    fi
    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_read_multi_vector_dedicated_file..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_multi_vector_dedicated_file -v; then
        echo -e "${GREEN}✓ Python multi-vector dedicated file read completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python multi-vector dedicated file read failed${NC}"
        return 1
    fi
}

run_multi_vector_dedicated_file_py_write_test() {
    echo -e "${YELLOW}=== Running Multi-Vector Dedicated File Test (Python Write, Java Read) ===${NC}"

    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_py_write_multi_vector_dedicated_file..."
    if ! python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_py_write_multi_vector_dedicated_file -v; then
        echo -e "${RED}✗ Python multi-vector dedicated file write failed${NC}"
        return 1
    fi
    echo -e "${GREEN}✓ Python multi-vector dedicated file write completed successfully${NC}"

    echo ""

    cd "$PROJECT_ROOT"
    echo "Running Maven test for JavaPyVortexE2ETest.testJavaReadMultiVectorDedicatedFile..."
    if mvn test -Dtest=org.apache.paimon.JavaPyVortexE2ETest#testJavaReadMultiVectorDedicatedFile -pl paimon-vortex/paimon-vortex-format -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java multi-vector dedicated file read completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Java multi-vector dedicated file read failed${NC}"
        return 1
    fi
}

# Function to run native full-text index test (Java write index, Python read and search)
run_native_fulltext_test() {
    echo -e "${YELLOW}=== Step 8: Running Native Full-Text Index Test (Java Write, Python Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyNativeFullTextE2ETest.testNativeFullTextIndexWrite..."
        if mvn test -Dtest=org.apache.paimon.fulltext.index.JavaPyNativeFullTextE2ETest#testNativeFullTextIndexWrite -pl paimon-full-text -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java test failed${NC}"
            return 1
        fi
    fi
    cd "$PAIMON_PYTHON_DIR"
    echo "Checking paimon-ftindex Python dependency for native full-text index reads..."
    if ! python -c "import paimon_ftindex"; then
        echo -e "${RED}✗ paimon-ftindex is not installed or its native FFI library is unavailable${NC}"
        return 1
    fi
    echo "Running Python test for JavaPyReadWriteTest.test_read_native_full_text_index..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_native_full_text_index -v; then
        echo -e "${GREEN}✓ Python test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python test failed${NC}"
        return 1
    fi
}

# Function to run Lumina vector index test (Java write index, Python read and search)
run_lumina_vector_test() {
    echo -e "${YELLOW}=== Step 9: Running Lumina Vector Index Test (Java Write, Python Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyLuminaE2ETest.testLuminaVectorIndexWrite..."
        if mvn test -Dtest=org.apache.paimon.lumina.index.JavaPyLuminaE2ETest#testLuminaVectorIndexWrite -pl paimon-lumina -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java test failed${NC}"
            return 1
        fi
    fi
    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_read_lumina_vector_index..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_lumina_vector_index -v; then
        echo -e "${GREEN}✓ Python test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python test failed${NC}"
        return 1
    fi
}

# Function to run Lumina vector + BTree pre-filter test.
run_lumina_vector_btree_test() {
    echo -e "${YELLOW}=== Running Lumina Vector + BTree Pre-Filter Test (Java Write, Python Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyLuminaE2ETest.testLuminaVectorWithBTreeIndexWrite..."
        if mvn test -Dtest=org.apache.paimon.lumina.index.JavaPyLuminaE2ETest#testLuminaVectorWithBTreeIndexWrite -pl paimon-lumina -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java test failed${NC}"
            return 1
        fi
    fi
    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_read_lumina_vector_with_btree_filter..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_lumina_vector_with_btree_filter -v; then
        echo -e "${GREEN}✓ Python test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python test failed${NC}"
        return 1
    fi
}

ensure_paimon_vindex() {
    if python -c "import paimon_vindex" >/dev/null 2>&1; then
        return 0
    fi

    echo "Installing Python paimon-vindex dependency..."
    if python -m pip install 'paimon-vindex==0.1.0'; then
        return 0
    fi

    echo -e "${YELLOW}Direct pip install failed; installing paimon-vindex into a temporary target directory...${NC}"
    local target_dir="${TMPDIR:-/tmp}/paimon-vindex-site"
    rm -rf "$target_dir"
    if python -m pip install --target "$target_dir" 'paimon-vindex==0.1.0'; then
        export PYTHONPATH="$target_dir:${PYTHONPATH:-}"
        return 0
    fi

    if python -c "import numpy" >/dev/null 2>&1; then
        echo -e "${YELLOW}Dependency install failed but numpy is already available; retrying paimon-vindex without dependencies...${NC}"
        rm -rf "$target_dir"
        if python -m pip install --target "$target_dir" --no-deps 'paimon-vindex==0.1.0'; then
            export PYTHONPATH="$target_dir:${PYTHONPATH:-}"
            return 0
        fi
    fi

    echo -e "${RED}✗ Failed to install paimon-vindex${NC}"
    return 1
}

# Function to run paimon-vindex vector index test (Java write index, Python read and search)
run_vindex_vector_test() {
    echo -e "${YELLOW}=== Running paimon-vindex Vector Index Test (Java Write, Python Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyE2ETest.testVindexVectorIndexWrite..."
        if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testVindexVectorIndexWrite -pl paimon-vector -am -q -DfailIfNoTests=false -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java test failed${NC}"
            return 1
        fi
    fi
    cd "$PAIMON_PYTHON_DIR"
    if ! ensure_paimon_vindex; then
        return 1
    fi
    echo "Running Python test for JavaPyReadWriteTest.test_read_vindex_vector_index..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_vindex_vector_index -v; then
        echo -e "${GREEN}✓ Python test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python test failed${NC}"
        return 1
    fi
}

run_vindex_vector_raw_fallback_test() {
    echo -e "${YELLOW}=== Running paimon-vindex Vector Raw Fallback Test (Java Write, Python Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyE2ETest.testVindexVectorRawFallbackWrite..."
        if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testVindexVectorRawFallbackWrite -pl paimon-vector -am -q -DfailIfNoTests=false -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java test failed${NC}"
            return 1
        fi
    fi
    cd "$PAIMON_PYTHON_DIR"
    if ! ensure_paimon_vindex; then
        return 1
    fi
    echo "Running Python test for JavaPyReadWriteTest.test_read_vindex_vector_raw_fallback..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_vindex_vector_raw_fallback -v; then
        echo -e "${GREEN}✓ Python test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python test failed${NC}"
        return 1
    fi
}

run_compact_conflict_test() {
    echo -e "${YELLOW}=== Running Compact Conflict Test (Java Write Base, Python Shard Update + Java Compact) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        # Step 1: Java writes 5 base files
        echo "Running Maven test for JavaPyE2ETest.testCompactConflictWriteBase..."
        if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testCompactConflictWriteBase -pl paimon-core -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java write base files completed successfully${NC}"
        else
            echo -e "${RED}✗ Java write base files failed${NC}"
            return 1
        fi
    fi

    # Step 2-4: Python shard update (scan -> Java compact -> commit conflict detected)
    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_compact_conflict_shard_update..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_compact_conflict_shard_update -v; then
        echo -e "${GREEN}✓ Python compact conflict test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python compact conflict test failed${NC}"
        return 1
    fi
}

run_blob_compact_conflict_test() {
    echo -e "${YELLOW}=== Running Blob Compact Conflict Test (Java Write Base, Python Blob Update + Java Compact) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyE2ETest.testBlobCompactConflictWriteBase..."
        if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testBlobCompactConflictWriteBase -pl paimon-core -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java write base blob files completed successfully${NC}"
        else
            echo -e "${RED}✗ Java write base blob files failed${NC}"
            return 1
        fi
    fi

    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_blob_compact_conflict_update..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_blob_compact_conflict_update -v; then
        echo -e "${GREEN}✓ Python blob compact conflict test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python blob compact conflict test failed${NC}"
        return 1
    fi
}

run_data_evolution_test() {
    echo -e "${YELLOW}=== Running Data Evolution Test (Java Write, Python Read) ===${NC}"

    local core_result=0
    local lance_result=0

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        # Java write data evolution tables (parquet/orc/avro)
        echo "Running Maven test for JavaPyE2ETest.testDataEvolutionWrite..."
        if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testDataEvolutionWrite -pl paimon-core -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java data evolution write (parquet/orc/avro) completed successfully${NC}"
        else
            echo -e "${RED}✗ Java data evolution write (parquet/orc/avro) failed${NC}"
            core_result=1
        fi

        # Java write data evolution table (lance) -- only read back on >=3.8.
        if [[ "$PYTHON_MINOR" -ge 8 ]]; then
            echo "Running Maven test for JavaPyLanceE2ETest.testDataEvolutionWriteLance..."
            if mvn test -Dtest=org.apache.paimon.JavaPyLanceE2ETest#testDataEvolutionWriteLance -pl paimon-lance -q -Drun.e2e.tests=true; then
                echo -e "${GREEN}✓ Java data evolution write (lance) completed successfully${NC}"
            else
                echo -e "${RED}✗ Java data evolution write (lance) failed${NC}"
                lance_result=1
            fi
        fi
    fi

    # Python read data evolution tables
    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_read_data_evolution_table..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest -k "test_read_data_evolution_table" -v; then
        echo -e "${GREEN}✓ Python data evolution read completed successfully${NC}"
    else
        echo -e "${RED}✗ Python data evolution read failed${NC}"
        return 1
    fi

    if [[ $core_result -ne 0 || $lance_result -ne 0 ]]; then
        return 1
    fi
    return 0
}

run_data_evolution_deletion_vector_test() {
    echo -e "${YELLOW}=== Running Data Evolution Deletion Vector Test (Java Write, Python Read) ===${NC}"

    cd "$PROJECT_ROOT"

    echo "Running Maven test for JavaPyE2ETest.testDataEvolutionDeletionVectorWrite..."
    if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testDataEvolutionDeletionVectorWrite -pl paimon-core -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java data evolution deletion vector write completed successfully${NC}"
    else
        echo -e "${RED}✗ Java data evolution deletion vector write failed${NC}"
        return 1
    fi

    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_read_data_evolution_deletion_vector_table..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_data_evolution_deletion_vector_table -v; then
        echo -e "${GREEN}✓ Python data evolution deletion vector read completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python data evolution deletion vector read failed${NC}"
        return 1
    fi
}

run_data_evolution_py_write_test() {
    echo -e "${YELLOW}=== Running Data Evolution Test (Python Write, Java Read) ===${NC}"

    cd "$PAIMON_PYTHON_DIR"

    # Python write data evolution tables
    echo "Running Python test for JavaPyReadWriteTest.test_py_write_data_evolution_table..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest -k "test_py_write_data_evolution_table" -v; then
        echo -e "${GREEN}✓ Python data evolution write completed successfully${NC}"
    else
        echo -e "${RED}✗ Python data evolution write failed${NC}"
        return 1
    fi

    cd "$PROJECT_ROOT"

    # Java read data evolution tables (parquet/orc/avro)
    echo "Running Maven test for JavaPyE2ETest.testReadDataEvolutionTable..."
    local core_result=0
    if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testReadDataEvolutionTable -pl paimon-core -q -Drun.e2e.tests=true -Dpython.version="$PYTHON_VERSION"; then
        echo -e "${GREEN}✓ Java data evolution read (parquet/orc/avro) completed successfully${NC}"
    else
        echo -e "${RED}✗ Java data evolution read (parquet/orc/avro) failed${NC}"
        core_result=1
    fi

    # Java read Lance reads a Python-written table; Python skips lance on <3.8.
    local lance_result=0
    if [[ "$PYTHON_MINOR" -ge 8 ]]; then
        echo "Running Maven test for JavaPyLanceE2ETest.testReadDataEvolutionTableLance..."
        if mvn test -Dtest=org.apache.paimon.JavaPyLanceE2ETest#testReadDataEvolutionTableLance -pl paimon-lance -q -Drun.e2e.tests=true -Dpython.version="$PYTHON_VERSION"; then
            echo -e "${GREEN}✓ Java data evolution read (lance) completed successfully${NC}"
        else
            echo -e "${RED}✗ Java data evolution read (lance) failed${NC}"
            lance_result=1
        fi
    else
        echo -e "${YELLOW}⏭ Skipping Java data evolution read Lance (lance needs Python >= 3.8, current: $PYTHON_VERSION)${NC}"
    fi

    if [[ $core_result -ne 0 || $lance_result -ne 0 ]]; then
        return 1
    fi
    return 0
}

run_blob_alter_compact_test() {
    echo -e "${YELLOW}=== Running Blob Alter+Compact Test (Java Write+Alter+Compact, Python Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyE2ETest.testBlobWriteAlterCompact..."
        if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testBlobWriteAlterCompact -pl paimon-core -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java blob write+alter+compact test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java blob write+alter+compact test failed${NC}"
            return 1
        fi
    fi
    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_read_blob_after_alter_and_compact..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_blob_after_alter_and_compact -v; then
        echo -e "${GREEN}✓ Python blob read test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python blob read test failed${NC}"
        return 1
    fi
}

run_array_blob_interop_test() {
    echo -e "${YELLOW}=== Running ARRAY<BLOB> Test (Java Write → Python Read, Python Write → Java Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"
        echo "Running Maven test for JavaPyE2ETest.testJavaWriteArrayBlobTable..."
        if ! mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testJavaWriteArrayBlobTable -pl paimon-core -q -Drun.e2e.tests=true; then
            echo -e "${RED}✗ Java ARRAY<BLOB> write test failed${NC}"
            return 1
        fi
        echo -e "${GREEN}✓ Java ARRAY<BLOB> write test completed successfully${NC}"
    fi

    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python ARRAY<BLOB> read test..."
    if ! python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_array_blob_written_by_java -v; then
        echo -e "${RED}✗ Python ARRAY<BLOB> read test failed${NC}"
        return 1
    fi
    echo -e "${GREEN}✓ Python ARRAY<BLOB> read test completed successfully${NC}"

    echo "Running Python ARRAY<BLOB> write test..."
    if ! python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_write_array_blob_for_java -v; then
        echo -e "${RED}✗ Python ARRAY<BLOB> write test failed${NC}"
        return 1
    fi
    echo -e "${GREEN}✓ Python ARRAY<BLOB> write test completed successfully${NC}"

    cd "$PROJECT_ROOT"
    echo "Running Maven test for JavaPyE2ETest.testJavaReadArrayBlobTable..."
    if ! mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testJavaReadArrayBlobTable -pl paimon-core -q -Drun.e2e.tests=true; then
        echo -e "${RED}✗ Java ARRAY<BLOB> read test failed${NC}"
        return 1
    fi
    echo -e "${GREEN}✓ Java ARRAY<BLOB> read test completed successfully${NC}"
}

run_map_blob_interop_test() {
    echo -e "${YELLOW}=== Running MAP<INT, BLOB> Test (Java Write → Python Read, Python Write → Java Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"
        echo "Running Maven test for JavaPyE2ETest.testJavaWriteMapBlobTable..."
        if ! mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testJavaWriteMapBlobTable -pl paimon-core -q -Drun.e2e.tests=true; then
            echo -e "${RED}✗ Java MAP<INT, BLOB> write test failed${NC}"
            return 1
        fi
        echo -e "${GREEN}✓ Java MAP<INT, BLOB> write test completed successfully${NC}"
    fi

    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python MAP<INT, BLOB> read test..."
    if ! python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_map_blob_written_by_java -v; then
        echo -e "${RED}✗ Python MAP<INT, BLOB> read test failed${NC}"
        return 1
    fi
    echo -e "${GREEN}✓ Python MAP<INT, BLOB> read test completed successfully${NC}"

    echo "Running Python MAP<INT, BLOB> write test..."
    if ! python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_write_map_blob_for_java -v; then
        echo -e "${RED}✗ Python MAP<INT, BLOB> write test failed${NC}"
        return 1
    fi
    echo -e "${GREEN}✓ Python MAP<INT, BLOB> write test completed successfully${NC}"

    cd "$PROJECT_ROOT"
    echo "Running Maven test for JavaPyE2ETest.testJavaReadMapBlobTable..."
    if ! mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testJavaReadMapBlobTable -pl paimon-core -q -Drun.e2e.tests=true; then
        echo -e "${RED}✗ Java MAP<INT, BLOB> read test failed${NC}"
        return 1
    fi
    echo -e "${GREEN}✓ Java MAP<INT, BLOB> read test completed successfully${NC}"
}

# Function to run VARIANT test (Java write, Python read)
run_java_variant_write_py_read_test() {
    echo -e "${YELLOW}=== Running VARIANT Test (Java Write, Python Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyE2ETest.testJavaWriteVariantTable..."
        if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testJavaWriteVariantTable -pl paimon-core -q -Drun.e2e.tests=true; then
            echo -e "${GREEN}✓ Java VARIANT write test completed successfully${NC}"
        else
            echo -e "${RED}✗ Java VARIANT write test failed${NC}"
            return 1
        fi
    fi
    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_py_read_variant_table..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_py_read_variant_table -v; then
        echo -e "${GREEN}✓ Python VARIANT read test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Python VARIANT read test failed${NC}"
        return 1
    fi
}

# Function to run VARIANT test (Python write, Java read)
run_py_variant_write_java_read_test() {
    echo -e "${YELLOW}=== Running VARIANT Test (Python Write, Java Read) ===${NC}"

    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_py_write_variant_table..."
    if ! python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_py_write_variant_table -v; then
        echo -e "${RED}✗ Python VARIANT write test failed${NC}"
        return 1
    fi
    echo -e "${GREEN}✓ Python VARIANT write test completed successfully${NC}"

    echo ""

    cd "$PROJECT_ROOT"
    echo "Running Maven test for JavaPyE2ETest.testJavaReadVariantTable..."
    if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testJavaReadVariantTable -pl paimon-core -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java VARIANT read test completed successfully${NC}"
        return 0
    else
        echo -e "${RED}✗ Java VARIANT read test failed${NC}"
        return 1
    fi
}

# Function to run ROW format test (Java write, Python read, Python write, Java read)
run_row_format_test() {
    echo -e "${YELLOW}=== Running ROW Format Test (Java Write → Python Read, Python Write → Java Read) ===${NC}"

    if ! skip_batched_java_write; then
        cd "$PROJECT_ROOT"

        echo "Running Maven test for JavaPyE2ETest.testJavaWriteRowAppendTable..."
        if ! mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testJavaWriteRowAppendTable -pl paimon-core -q -Drun.e2e.tests=true; then
            echo -e "${RED}✗ Java ROW write test failed${NC}"
            return 1
        fi
        echo -e "${GREEN}✓ Java ROW write test completed successfully${NC}"
    fi

    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_read_row_append_table..."
    if ! python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_row_append_table -v; then
        echo -e "${RED}✗ Python ROW read test failed${NC}"
        return 1
    fi
    echo -e "${GREEN}✓ Python ROW read test completed successfully${NC}"

    echo ""

    echo "Running Python test for JavaPyReadWriteTest.test_py_write_row_append_table..."
    if ! python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_py_write_row_append_table -v; then
        echo -e "${RED}✗ Python ROW write test failed${NC}"
        return 1
    fi
    echo -e "${GREEN}✓ Python ROW write test completed successfully${NC}"

    echo ""

    cd "$PROJECT_ROOT"
    echo "Running Maven test for JavaPyE2ETest.testReadRowAppendTable..."
    if ! mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testReadRowAppendTable -pl paimon-core -q -Drun.e2e.tests=true; then
        echo -e "${RED}✗ Java ROW read test failed${NC}"
        return 1
    fi
    echo -e "${GREEN}✓ Java ROW read test completed successfully${NC}"
    return 0
}

# Main execution
main() {
    local java_write_result=0
    local python_read_result=0
    local python_write_result=0
    local java_read_result=0
    local pk_dv_result=0
    local btree_index_result=0
    local btree_raw_fallback_result=0
    local bitmap_index_result=0
    local compressed_global_index_result=0
    local compressed_text_result=0
    local vector_append_table_result=0
    local native_fulltext_result=0
    local lumina_vector_result=0
    local lumina_vector_btree_result=0
    local vindex_vector_result=0
    local vindex_vector_raw_fallback_result=0
    local compact_conflict_result=0
    local blob_compact_conflict_result=0
    local blob_alter_compact_result=0
    local array_blob_interop_result=0
    local map_blob_interop_result=0
    local data_evolution_result=0
    local data_evolution_deletion_vector_result=0
    local data_evolution_py_write_result=0
    local vector_dedicated_java_write_result=0
    local vector_dedicated_py_write_result=0
    local multi_vector_dedicated_java_write_result=0
    local multi_vector_dedicated_py_write_result=0
    local java_variant_write_py_read_result=0
    local py_variant_write_java_read_result=0
    local row_format_result=0

    # Detect Python version
    PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo "unknown")
    PYTHON_MINOR=$(python -c "import sys; print(sys.version_info.minor)" 2>/dev/null || echo "0")
    echo "Detected Python version: $PYTHON_VERSION"

    echo -e "${YELLOW}Starting mixed language test execution...${NC}"
    echo ""

    if java_write_batch_enabled; then
        if run_batched_java_write_tests; then
            JAVA_WRITE_BATCH_DONE=true
            echo -e "${GREEN}✓ Batched Java write phase completed; later Java write steps will be skipped${NC}"
        else
            JAVA_WRITE_BATCH_DONE=false
            echo -e "${RED}✗ Batched Java write phase failed${NC}"
            echo -e "${YELLOW}Rerun with MIXED_TESTS_BATCH_JAVA_WRITES=false for the original one-by-one Java write flow.${NC}"
            return 1
        fi
        echo ""
    fi

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

    # Run Java read test (handles both Parquet/Orc/Avro and Lance)
    if ! run_java_read_test; then
        java_read_result=1
    fi

    # Run pk dv read test
    if ! run_pk_dv_test; then
        pk_dv_result=1
    fi

    echo ""

    # Run BTree index test (Java write, Python read)
    if ! run_btree_index_test; then
        btree_index_result=1
    fi

    echo ""

    # Raw fallback / bitmap / global index need pyroaring BitMap64 (>=1.0,
    # Python >=3.8) on the Python read side; skip on older interpreters.
    if [[ "$PYTHON_MINOR" -ge 8 ]]; then
        # Run BTree raw fallback test (Java write indexed + unindexed rows, Python read)
        if ! run_btree_raw_fallback_test; then
            btree_raw_fallback_result=1
        fi

        echo ""

        # Run Bitmap index test (Java write, Python read)
        if ! run_bitmap_index_test; then
            bitmap_index_result=1
        fi

        echo ""

        if ! run_compressed_global_index_test; then
            compressed_global_index_result=1
        fi
    else
        echo -e "${YELLOW}⏭ Skipping BTree raw fallback / bitmap / global index tests (need BitMap64, Python >= 3.8, current: $PYTHON_VERSION)${NC}"
        btree_raw_fallback_result=0
        bitmap_index_result=0
        compressed_global_index_result=0
    fi

    echo ""

    if ! run_compressed_text_test; then
        compressed_text_result=1
    fi

    echo ""

    # Run Vector append table test (Java write, Python read)
    if ! run_vector_append_table_test; then
        vector_append_table_result=1
    fi

    echo ""

    # Run Vector dedicated file tests (requires Python >= 3.11 for vortex-data)
    if [[ "$PYTHON_MINOR" -ge 11 ]]; then
        # Run Vector dedicated file test (Java write, Python read)
        if ! run_vector_dedicated_file_java_write_test; then
            vector_dedicated_java_write_result=1
        fi

        echo ""

        # Run Vector dedicated file test (Python write, Java read)
        if ! run_vector_dedicated_file_py_write_test; then
            vector_dedicated_py_write_result=1
        fi

        echo ""

        # Run Multi-Vector dedicated file test (Java write, Python read)
        if ! run_multi_vector_dedicated_file_java_write_test; then
            multi_vector_dedicated_java_write_result=1
        fi

        echo ""

        # Run Multi-Vector dedicated file test (Python write, Java read)
        if ! run_multi_vector_dedicated_file_py_write_test; then
            multi_vector_dedicated_py_write_result=1
        fi
    else
        echo -e "${YELLOW}⏭ Skipping Vector Dedicated File Tests (requires Python >= 3.11 for vortex, current: $PYTHON_VERSION)${NC}"
        vector_dedicated_java_write_result=0
        vector_dedicated_py_write_result=0
        multi_vector_dedicated_java_write_result=0
        multi_vector_dedicated_py_write_result=0
    fi

    echo ""

    # Run native full-text index test (requires Python >= 3.8)
    if [[ "$PYTHON_MINOR" -ge 8 ]]; then
        if ! run_native_fulltext_test; then
            native_fulltext_result=1
        fi
    else
        echo -e "${YELLOW}⏭ Skipping Native Full-Text Index Test (requires Python >= 3.8, current: $PYTHON_VERSION)${NC}"
        native_fulltext_result=0
    fi

    echo ""

    # Lumina vector index needs BitMap64 (Python >= 3.8) on the read side.
    if [[ "$PYTHON_MINOR" -ge 8 ]]; then
        # Run Lumina vector index test (Java write, Python read)
        if ! run_lumina_vector_test; then
            lumina_vector_result=1
        fi

        echo ""

        # Run Lumina vector + BTree pre-filter test (Java write, Python read)
        if ! run_lumina_vector_btree_test; then
            lumina_vector_btree_result=1
        fi
    else
        echo -e "${YELLOW}⏭ Skipping Lumina Vector Index tests (need BitMap64, Python >= 3.8, current: $PYTHON_VERSION)${NC}"
        lumina_vector_result=0
        lumina_vector_btree_result=0
    fi

    echo ""

    # Run paimon-vindex vector index test (requires Python >= 3.9)
    if [[ "$PYTHON_MINOR" -ge 9 ]]; then
        if ! run_vindex_vector_test; then
            vindex_vector_result=1
        fi

        echo ""

        if ! run_vindex_vector_raw_fallback_test; then
            vindex_vector_raw_fallback_result=1
        fi
    else
        echo -e "${YELLOW}⏭ Skipping paimon-vindex Vector Index Test (requires Python >= 3.9, current: $PYTHON_VERSION)${NC}"
        vindex_vector_result=0
        vindex_vector_raw_fallback_result=0
    fi

    echo ""

    # Run compact conflict test (Java write+compact, Python read)
    if ! run_compact_conflict_test; then
        compact_conflict_result=1
    fi

    echo ""

    # Run blob compact conflict test (Java write base + Python blob update + Java compact)
    if ! run_blob_compact_conflict_test; then
        blob_compact_conflict_result=1
    fi

    echo ""

    # Run blob alter+compact test (Java write+alter+compact, Python read)
    if ! run_blob_alter_compact_test; then
        blob_alter_compact_result=1
    fi

    echo ""

    if ! run_array_blob_interop_test; then
        array_blob_interop_result=1
    fi

    echo ""

    if ! run_map_blob_interop_test; then
        map_blob_interop_result=1
    fi

    echo ""

    # Run data evolution test (Java write, Python read). Lance variant skips
    # itself on <3.8 (get_file_format_params + gated Java lance read).
    if ! run_data_evolution_test; then
        data_evolution_result=1
    fi

    echo ""

    # Run data evolution deletion vector test (Java write, Python read)
    if ! run_data_evolution_deletion_vector_test; then
        data_evolution_deletion_vector_result=1
    fi

    echo ""

    # Run data evolution test (Python write, Java read)
    if ! run_data_evolution_py_write_test; then
        data_evolution_py_write_result=1
    fi

    echo ""

    # Run VARIANT type tests (requires Python >= 3.7)
    if [[ "$PYTHON_MINOR" -ge 7 ]]; then
        if ! run_java_variant_write_py_read_test; then
            java_variant_write_py_read_result=1
        fi

        echo ""

        if ! run_py_variant_write_java_read_test; then
            py_variant_write_java_read_result=1
        fi
    else
        echo -e "${YELLOW}⏭ Skipping VARIANT Type Tests (requires Python >= 3.7, current: $PYTHON_VERSION)${NC}"
        java_variant_write_py_read_result=0
        py_variant_write_java_read_result=0
    fi

    echo ""

    # Run ROW format test (Java write + Python read + Python write + Java read)
    if ! run_row_format_test; then
        row_format_result=1
    fi

    echo ""

    echo -e "${YELLOW}=== Test Results Summary ===${NC}"

    if [[ $java_write_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Java Write Test (Parquet/Orc/Avro + Lance): PASSED${NC}"
    else
        echo -e "${RED}✗ Java Write Test (Parquet/Orc/Avro + Lance): FAILED${NC}"
    fi

    if [[ $python_read_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Python Read Test (JavaPyReadWriteTest.test_read_pk_table): PASSED${NC}"
    else
        echo -e "${RED}✗ Python Read Test (JavaPyReadWriteTest.test_read_pk_table): FAILED${NC}"
    fi

    if [[ $python_write_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Python Write Test (test_py_write_read_pk_table): PASSED${NC}"
    else
        echo -e "${RED}✗ Python Write Test (test_py_write_read_pk_table): FAILED${NC}"
    fi

    if [[ $java_read_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Java Read Test (Parquet/Orc/Avro + Lance): PASSED${NC}"
    else
        echo -e "${RED}✗ Java Read Test (Parquet/Orc/Avro + Lance): FAILED${NC}"
    fi

    if [[ $pk_dv_result -eq 0 ]]; then
        echo -e "${GREEN}✓ PK DV Test (JavaPyReadWriteTest.testPKDeletionVectorWriteRead): PASSED${NC}"
    else
        echo -e "${RED}✗ PK DV Test (JavaPyReadWriteTest.testPKDeletionVectorWriteRead): FAILED${NC}"
    fi

    if [[ $btree_index_result -eq 0 ]]; then
        echo -e "${GREEN}✓ BTree Index Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ BTree Index Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $btree_raw_fallback_result -eq 0 ]]; then
        echo -e "${GREEN}✓ BTree Raw Fallback Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ BTree Raw Fallback Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $bitmap_index_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Bitmap Index Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Bitmap Index Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $compressed_global_index_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Compressed Global Index Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Compressed Global Index Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $compressed_text_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Compressed Text Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Compressed Text Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $vector_append_table_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Vector Append Table Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Vector Append Table Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $vector_dedicated_java_write_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Vector Dedicated File Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Vector Dedicated File Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $vector_dedicated_py_write_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Vector Dedicated File Test (Python Write, Java Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Vector Dedicated File Test (Python Write, Java Read): FAILED${NC}"
    fi

    if [[ $multi_vector_dedicated_java_write_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Multi-Vector Dedicated File Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Multi-Vector Dedicated File Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $multi_vector_dedicated_py_write_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Multi-Vector Dedicated File Test (Python Write, Java Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Multi-Vector Dedicated File Test (Python Write, Java Read): FAILED${NC}"
    fi

    if [[ $native_fulltext_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Native Full-Text Index Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Native Full-Text Index Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $lumina_vector_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Lumina Vector Index Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Lumina Vector Index Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $lumina_vector_btree_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Lumina Vector + BTree Pre-Filter Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Lumina Vector + BTree Pre-Filter Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $vindex_vector_result -eq 0 ]]; then
        echo -e "${GREEN}✓ paimon-vindex Vector Index Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ paimon-vindex Vector Index Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $vindex_vector_raw_fallback_result -eq 0 ]]; then
        echo -e "${GREEN}✓ paimon-vindex Vector Raw Fallback Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ paimon-vindex Vector Raw Fallback Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $compact_conflict_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Compact Conflict Test (Java Write+Compact, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Compact Conflict Test (Java Write+Compact, Python Read): FAILED${NC}"
    fi

    if [[ $blob_compact_conflict_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Blob Compact Conflict Test (Java Write+Compact, Python Blob Update): PASSED${NC}"
    else
        echo -e "${RED}✗ Blob Compact Conflict Test (Java Write+Compact, Python Blob Update): FAILED${NC}"
    fi

    if [[ $blob_alter_compact_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Blob Alter+Compact Test (Java Write+Alter+Compact, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Blob Alter+Compact Test (Java Write+Alter+Compact, Python Read): FAILED${NC}"
    fi

    if [[ $array_blob_interop_result -eq 0 ]]; then
        echo -e "${GREEN}✓ ARRAY<BLOB> Interoperability Test (Java ↔ Python): PASSED${NC}"
    else
        echo -e "${RED}✗ ARRAY<BLOB> Interoperability Test (Java ↔ Python): FAILED${NC}"
    fi

    if [[ $map_blob_interop_result -eq 0 ]]; then
        echo -e "${GREEN}✓ MAP<INT, BLOB> Interoperability Test (Java ↔ Python): PASSED${NC}"
    else
        echo -e "${RED}✗ MAP<INT, BLOB> Interoperability Test (Java ↔ Python): FAILED${NC}"
    fi

    if [[ $data_evolution_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Data Evolution Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Data Evolution Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $data_evolution_deletion_vector_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Data Evolution Deletion Vector Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Data Evolution Deletion Vector Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $data_evolution_py_write_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Data Evolution Test (Python Write, Java Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Data Evolution Test (Python Write, Java Read): FAILED${NC}"
    fi

    if [[ $java_variant_write_py_read_result -eq 0 ]]; then
        echo -e "${GREEN}✓ VARIANT Type Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ VARIANT Type Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $py_variant_write_java_read_result -eq 0 ]]; then
        echo -e "${GREEN}✓ VARIANT Type Test (Python Write, Java Read): PASSED${NC}"
    else
        echo -e "${RED}✗ VARIANT Type Test (Python Write, Java Read): FAILED${NC}"
    fi

    if [[ $row_format_result -eq 0 ]]; then
        echo -e "${GREEN}✓ ROW Format Test (Java Write ↔ Python Read/Write): PASSED${NC}"
    else
        echo -e "${RED}✗ ROW Format Test (Java Write ↔ Python Read/Write): FAILED${NC}"
    fi

    echo ""

    # Clean up warehouse directory after all tests
    cleanup_warehouse

    if [[ $java_write_result -eq 0 && $python_read_result -eq 0 && $python_write_result -eq 0 && $java_read_result -eq 0 && $pk_dv_result -eq 0 && $btree_index_result -eq 0 && $btree_raw_fallback_result -eq 0 && $bitmap_index_result -eq 0 && $compressed_global_index_result -eq 0 && $compressed_text_result -eq 0 && $native_fulltext_result -eq 0 && $lumina_vector_result -eq 0 && $lumina_vector_btree_result -eq 0 && $vindex_vector_result -eq 0 && $vindex_vector_raw_fallback_result -eq 0 && $compact_conflict_result -eq 0 && $blob_compact_conflict_result -eq 0 && $blob_alter_compact_result -eq 0 && $array_blob_interop_result -eq 0 && $map_blob_interop_result -eq 0 && $data_evolution_result -eq 0 && $data_evolution_deletion_vector_result -eq 0 && $data_evolution_py_write_result -eq 0 && $java_variant_write_py_read_result -eq 0 && $py_variant_write_java_read_result -eq 0 && $vector_append_table_result -eq 0 && $vector_dedicated_java_write_result -eq 0 && $vector_dedicated_py_write_result -eq 0 && $multi_vector_dedicated_java_write_result -eq 0 && $multi_vector_dedicated_py_write_result -eq 0 && $row_format_result -eq 0 ]]; then
        echo -e "${GREEN}🎉 All tests passed! Java-Python interoperability verified.${NC}"
        return 0
    else
        echo -e "${RED}❌ Some tests failed. Please check the output above.${NC}"
        return 1
    fi
}

# Run main function
main "$@"
