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
    echo -e "${YELLOW}=== Step 1: Running Java Write Tests (Parquet/Orc/Avro + Lance) ===${NC}"

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

# Function to run BTree index test (Java write, Python read)
run_btree_index_test() {
    echo -e "${YELLOW}=== Step 6: Running BTree Index Test (Java Write, Python Read) ===${NC}"

    cd "$PROJECT_ROOT"

    echo "Running Maven test for JavaPyE2ETest.testBtreeIndexWrite..."
    if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testBtreeIndexWrite -pl paimon-core -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java test completed successfully${NC}"
    else
        echo -e "${RED}✗ Java test failed${NC}"
        return 1
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

run_compressed_text_test() {
    echo -e "${YELLOW}=== Step 7: Running Compressed Text Test (Java Write, Python Read) ===${NC}"

    cd "$PROJECT_ROOT"

    echo "Running Maven test for JavaPyE2ETest.testJavaWriteCompressedTextAppendTable..."
    if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testJavaWriteCompressedTextAppendTable -pl paimon-core -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java test completed successfully${NC}"
    else
        echo -e "${RED}✗ Java test failed${NC}"
        return 1
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

# Function to run Tantivy full-text index test (Java write index, Python read and search)
run_tantivy_fulltext_test() {
    echo -e "${YELLOW}=== Step 8: Running Tantivy Full-Text Index Test (Java Write, Python Read) ===${NC}"

    cd "$PROJECT_ROOT"

    echo "Running Maven test for JavaPyTantivyE2ETest.testTantivyFullTextIndexWrite..."
    if mvn test -Dtest=org.apache.paimon.tantivy.index.JavaPyTantivyE2ETest#testTantivyFullTextIndexWrite -pl paimon-tantivy/paimon-tantivy-index -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java test completed successfully${NC}"
    else
        echo -e "${RED}✗ Java test failed${NC}"
        return 1
    fi
    cd "$PAIMON_PYTHON_DIR"
    echo "Running Python test for JavaPyReadWriteTest.test_read_tantivy_full_text_index..."
    if python -m pytest java_py_read_write_test.py::JavaPyReadWriteTest::test_read_tantivy_full_text_index -v; then
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

    cd "$PROJECT_ROOT"

    echo "Running Maven test for JavaPyLuminaE2ETest.testLuminaVectorIndexWrite..."
    if mvn test -Dtest=org.apache.paimon.lumina.index.JavaPyLuminaE2ETest#testLuminaVectorIndexWrite -pl paimon-lumina -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java test completed successfully${NC}"
    else
        echo -e "${RED}✗ Java test failed${NC}"
        return 1
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

    cd "$PROJECT_ROOT"

    echo "Running Maven test for JavaPyLuminaE2ETest.testLuminaVectorWithBTreeIndexWrite..."
    if mvn test -Dtest=org.apache.paimon.lumina.index.JavaPyLuminaE2ETest#testLuminaVectorWithBTreeIndexWrite -pl paimon-lumina -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java test completed successfully${NC}"
    else
        echo -e "${RED}✗ Java test failed${NC}"
        return 1
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

run_compact_conflict_test() {
    echo -e "${YELLOW}=== Running Compact Conflict Test (Java Write Base, Python Shard Update + Java Compact) ===${NC}"

    cd "$PROJECT_ROOT"

    # Step 1: Java writes 5 base files
    echo "Running Maven test for JavaPyE2ETest.testCompactConflictWriteBase..."
    if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testCompactConflictWriteBase -pl paimon-core -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java write base files completed successfully${NC}"
    else
        echo -e "${RED}✗ Java write base files failed${NC}"
        return 1
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

run_data_evolution_test() {
    echo -e "${YELLOW}=== Running Data Evolution Test (Java Write, Python Read) ===${NC}"

    cd "$PROJECT_ROOT"

    # Java write data evolution tables (parquet/orc/avro)
    echo "Running Maven test for JavaPyE2ETest.testDataEvolutionWrite..."
    local core_result=0
    if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testDataEvolutionWrite -pl paimon-core -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java data evolution write (parquet/orc/avro) completed successfully${NC}"
    else
        echo -e "${RED}✗ Java data evolution write (parquet/orc/avro) failed${NC}"
        core_result=1
    fi

    # Java write data evolution table (lance)
    echo "Running Maven test for JavaPyLanceE2ETest.testDataEvolutionWriteLance..."
    local lance_result=0
    if mvn test -Dtest=org.apache.paimon.JavaPyLanceE2ETest#testDataEvolutionWriteLance -pl paimon-lance -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java data evolution write (lance) completed successfully${NC}"
    else
        echo -e "${RED}✗ Java data evolution write (lance) failed${NC}"
        lance_result=1
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

    # Java read data evolution table (lance)
    echo "Running Maven test for JavaPyLanceE2ETest.testReadDataEvolutionTableLance..."
    local lance_result=0
    if mvn test -Dtest=org.apache.paimon.JavaPyLanceE2ETest#testReadDataEvolutionTableLance -pl paimon-lance -q -Drun.e2e.tests=true -Dpython.version="$PYTHON_VERSION"; then
        echo -e "${GREEN}✓ Java data evolution read (lance) completed successfully${NC}"
    else
        echo -e "${RED}✗ Java data evolution read (lance) failed${NC}"
        lance_result=1
    fi

    if [[ $core_result -ne 0 || $lance_result -ne 0 ]]; then
        return 1
    fi
    return 0
}

run_blob_alter_compact_test() {
    echo -e "${YELLOW}=== Running Blob Alter+Compact Test (Java Write+Alter+Compact, Python Read) ===${NC}"

    cd "$PROJECT_ROOT"

    echo "Running Maven test for JavaPyE2ETest.testBlobWriteAlterCompact..."
    if mvn test -Dtest=org.apache.paimon.JavaPyE2ETest#testBlobWriteAlterCompact -pl paimon-core -q -Drun.e2e.tests=true; then
        echo -e "${GREEN}✓ Java blob write+alter+compact test completed successfully${NC}"
    else
        echo -e "${RED}✗ Java blob write+alter+compact test failed${NC}"
        return 1
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

# Main execution
main() {
    local java_write_result=0
    local python_read_result=0
    local python_write_result=0
    local java_read_result=0
    local pk_dv_result=0
    local btree_index_result=0
    local compressed_text_result=0
    local tantivy_fulltext_result=0
    local lumina_vector_result=0
    local lumina_vector_btree_result=0
    local compact_conflict_result=0
    local blob_alter_compact_result=0
    local data_evolution_result=0
    local data_evolution_py_write_result=0

    # Detect Python version
    PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null || echo "unknown")
    PYTHON_MINOR=$(python -c "import sys; print(sys.version_info.minor)" 2>/dev/null || echo "0")
    echo "Detected Python version: $PYTHON_VERSION"

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

    if ! run_compressed_text_test; then
        compressed_text_result=1
    fi

    echo ""

    # Run Tantivy full-text index test (requires Python >= 3.10)
    if [[ "$PYTHON_MINOR" -ge 10 ]]; then
        if ! run_tantivy_fulltext_test; then
            tantivy_fulltext_result=1
        fi
    else
        echo -e "${YELLOW}⏭ Skipping Tantivy Full-Text Index Test (requires Python >= 3.10, current: $PYTHON_VERSION)${NC}"
        tantivy_fulltext_result=0
    fi

    echo ""

    # Run Lumina vector index test (Java write, Python read)
    if ! run_lumina_vector_test; then
        lumina_vector_result=1
    fi

    echo ""

    # Run Lumina vector + BTree pre-filter test (Java write, Python read)
    if ! run_lumina_vector_btree_test; then
        lumina_vector_btree_result=1
    fi

    echo ""

    # Run compact conflict test (Java write+compact, Python read)
    if ! run_compact_conflict_test; then
        compact_conflict_result=1
    fi

    echo ""

    # Run blob alter+compact test (Java write+alter+compact, Python read)
    if ! run_blob_alter_compact_test; then
        blob_alter_compact_result=1
    fi

    echo ""

    # Run data evolution test (Java write, Python read)
    if ! run_data_evolution_test; then
        data_evolution_result=1
    fi

    echo ""

    # Run data evolution test (Python write, Java read)
    if ! run_data_evolution_py_write_test; then
        data_evolution_py_write_result=1
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

    if [[ $compressed_text_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Compressed Text Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Compressed Text Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $tantivy_fulltext_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Tantivy Full-Text Index Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Tantivy Full-Text Index Test (Java Write, Python Read): FAILED${NC}"
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

    if [[ $compact_conflict_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Compact Conflict Test (Java Write+Compact, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Compact Conflict Test (Java Write+Compact, Python Read): FAILED${NC}"
    fi

    if [[ $blob_alter_compact_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Blob Alter+Compact Test (Java Write+Alter+Compact, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Blob Alter+Compact Test (Java Write+Alter+Compact, Python Read): FAILED${NC}"
    fi

    if [[ $data_evolution_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Data Evolution Test (Java Write, Python Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Data Evolution Test (Java Write, Python Read): FAILED${NC}"
    fi

    if [[ $data_evolution_py_write_result -eq 0 ]]; then
        echo -e "${GREEN}✓ Data Evolution Test (Python Write, Java Read): PASSED${NC}"
    else
        echo -e "${RED}✗ Data Evolution Test (Python Write, Java Read): FAILED${NC}"
    fi

    echo ""

    # Clean up warehouse directory after all tests
    cleanup_warehouse

    if [[ $java_write_result -eq 0 && $python_read_result -eq 0 && $python_write_result -eq 0 && $java_read_result -eq 0 && $pk_dv_result -eq 0 && $btree_index_result -eq 0 && $compressed_text_result -eq 0 && $tantivy_fulltext_result -eq 0 && $lumina_vector_result -eq 0 && $lumina_vector_btree_result -eq 0 && $compact_conflict_result -eq 0 && $blob_alter_compact_result -eq 0 && $data_evolution_result -eq 0 && $data_evolution_py_write_result -eq 0 ]]; then
        echo -e "${GREEN}🎉 All tests passed! Java-Python interoperability verified.${NC}"
        return 0
    else
        echo -e "${RED}❌ Some tests failed. Please check the output above.${NC}"
        return 1
    fi
}

# Run main function
main "$@"