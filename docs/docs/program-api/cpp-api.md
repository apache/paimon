---
title: "C++ API"
sidebar_position: 8
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# C++ API

[Paimon C++](https://github.com/apache/paimon-cpp) provides native table access for C++ applications
and engines. It exchanges columnar data through the Arrow C Data Interface.

This walkthrough follows the same workflow as the Java API: create a catalog and table, prepare and
commit a batch, then plan splits and read them. The snippets are function-body fragments returning
`paimon::Status` (place includes at file scope); `PAIMON_RETURN_NOT_OK` and `PAIMON_ASSIGN_OR_RAISE` propagate failures to its caller.
The `PrepareData` helper returns `arrow::Result` and uses Arrow's error macros instead.

For complete headers and a runnable application, start with the
[C++ examples](https://paimon.apache.org/docs/cpp/examples/index.html). The C++ project has its own
release cycle; use its build instructions and API reference for the version you select.

## Environment Settings

Follow the [C++ build guide](https://paimon.apache.org/docs/cpp/building.html) to install prerequisites
and select optional filesystem, file-format, and catalog components. A basic source build is:

```sh
git clone https://github.com/apache/paimon-cpp.git
cd paimon-cpp
mkdir build-release
cd build-release
cmake ..
make -j8       # if you have 8 CPU cores, otherwise adjust
make install
```

## Create Catalog

Create a filesystem catalog for a warehouse. Reuse these options and identifiers in the following
fragments; choose a fresh table name when running the walkthrough again.

```c++
#include "paimon/catalog/catalog.h"

const std::string root_path = "/tmp/paimon-cpp-warehouse";
const std::string db_name = "my_db";
const std::string table_name = "my_table";
std::map<std::string, std::string> options;
PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::Catalog> catalog,
                       paimon::Catalog::Create(root_path, options));
```

C++ also supports a REST catalog when built with `PAIMON_ENABLE_REST=ON`.
See the [C++ catalog guide](https://paimon.apache.org/docs/cpp/user_guide/catalog.html) for options
and supported operations. For REST-managed tables, use the table location returned by the catalog;
the path construction below is specific to the filesystem catalog.

## Create Database

Create the database before the table:

```c++
PAIMON_RETURN_NOT_OK(catalog->CreateDatabase(db_name, options, /*ignore_if_exists=*/false));
```

## Create Table

Define fields using an `arrow::Schema`, then export it through the Arrow C Data Interface.
This example creates an unpartitioned append table without primary keys.

```c++
arrow::FieldVector fields = {
    arrow::field("f0", arrow::utf8()),
    arrow::field("f1", arrow::int32()),
    arrow::field("f2", arrow::int32()),
    arrow::field("f3", arrow::float64()),
};
std::shared_ptr<arrow::Schema> schema = arrow::schema(fields);
::ArrowSchema arrow_schema;
arrow::Status arrow_status = arrow::ExportSchema(*schema, &arrow_schema);
if (!arrow_status.ok()) {
    return paimon::Status::Invalid(arrow_status.message());
}
PAIMON_RETURN_NOT_OK(catalog->CreateTable(paimon::Identifier(db_name, table_name),
                                            &arrow_schema,
                                            /*partition_keys=*/{},
                                            /*primary_keys=*/{}, options,
                                            /*ignore_if_exists=*/false));
```

See [Data Types](https://paimon.apache.org/docs/cpp/user_guide/data_types.html) for all supported
`arrow-to-paimon` data types mapping.

## Batch Write

First construct an Arrow batch, then write it and prepare commit messages. The committer publishes
the prepared changes. In a distributed application, collect messages from the participating writers
before committing. See the [memory format guide](https://paimon.apache.org/docs/cpp/user_guide/arrow.html)
for ownership and Arrow conversion details.

### Build a batch

```c++
arrow::Result<std::shared_ptr<arrow::StructArray>> PrepareData(const arrow::FieldVector& fields) {
    arrow::StringBuilder f0_builder;
    arrow::Int32Builder f1_builder;
    arrow::Int32Builder f2_builder;
    arrow::DoubleBuilder f3_builder;

    std::vector<std::tuple<std::string, int, int, double>> data = {
        {"Alice", 1, 0, 11.0}, {"Bob", 1, 1, 12.1}, {"Cathy", 1, 2, 13.2}};

    for (const auto& row : data) {
        ARROW_RETURN_NOT_OK(f0_builder.Append(std::get<0>(row)));
        ARROW_RETURN_NOT_OK(f1_builder.Append(std::get<1>(row)));
        ARROW_RETURN_NOT_OK(f2_builder.Append(std::get<2>(row)));
        ARROW_RETURN_NOT_OK(f3_builder.Append(std::get<3>(row)));
    }

    std::shared_ptr<arrow::Array> f0_array, f1_array, f2_array, f3_array;
    ARROW_RETURN_NOT_OK(f0_builder.Finish(&f0_array));
    ARROW_RETURN_NOT_OK(f1_builder.Finish(&f1_array));
    ARROW_RETURN_NOT_OK(f2_builder.Finish(&f2_array));
    ARROW_RETURN_NOT_OK(f3_builder.Finish(&f3_array));

    std::vector<std::shared_ptr<arrow::Array>> children = {f0_array, f1_array, f2_array, f3_array};
    auto struct_type = arrow::struct_(fields);
    return std::make_shared<arrow::StructArray>(struct_type, f0_array->length(), children);
}
```

### Write and commit

```c++
std::string table_path = root_path + "/" + db_name + ".db/" + table_name;
std::string commit_user = "some_commit_user";
// write
paimon::WriteContextBuilder context_builder(table_path, commit_user);
PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::WriteContext> write_context,
                        context_builder.SetOptions(options).Finish());
PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::FileStoreWrite> writer,
                        paimon::FileStoreWrite::Create(std::move(write_context)));
// prepare data
auto struct_array = PrepareData(fields);
if (!struct_array.ok()) {
    return paimon::Status::Invalid(struct_array.status().ToString());
}
::ArrowArray arrow_array;
arrow_status = arrow::ExportArray(*struct_array.ValueUnsafe(), &arrow_array);
if (!arrow_status.ok()) {
    return paimon::Status::Invalid(arrow_status.message());
}
paimon::RecordBatchBuilder batch_builder(&arrow_array);
PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::RecordBatch> record_batch,
                        batch_builder.Finish());
PAIMON_RETURN_NOT_OK(writer->Write(std::move(record_batch)));
PAIMON_ASSIGN_OR_RAISE(std::vector<std::shared_ptr<paimon::CommitMessage>> commit_message,
                        writer->PrepareCommit());

// commit
paimon::CommitContextBuilder commit_context_builder(table_path, commit_user);
PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::CommitContext> commit_context,
                        commit_context_builder.SetOptions(options).Finish());
PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::FileStoreCommit> committer,
                        paimon::FileStoreCommit::Create(std::move(commit_context)));
PAIMON_RETURN_NOT_OK(committer->Commit(commit_message));
```

## Batch Read

Configure the reader, plan the splits, then consume each batch. When distributing reads, plan once
and assign splits to reader tasks.

### Predicate pushdown

Use `ReadContextBuilder` to configure the reader. `EnablePredicateFilter(true)` requests row-level
filtering as well as any pruning supported by the reader:

```c++
// Example filter: 'f3' > 12.0 OR 'f1' == 1
PAIMON_ASSIGN_OR_RAISE(
    auto predicate,
    paimon::PredicateBuilder::Or({
        paimon::PredicateBuilder::GreaterThan(
            /*field_index=*/3, /*field_name=*/"f3",
            paimon::FieldType::DOUBLE, paimon::Literal(12.0)),
        paimon::PredicateBuilder::Equal(
            /*field_index=*/1, /*field_name=*/"f1",
            paimon::FieldType::INT, paimon::Literal(1))}));
paimon::ReadContextBuilder read_context_builder(table_path);
read_context_builder.SetPredicate(predicate).EnablePredicateFilter(true);
```

Set the projected fields on the same read context:

```c++
// Return f3, f1, and f2, in that order
read_context_builder.SetReadFieldNames({"f3", "f1", "f2"});
```

### Generate Splits

Create a scan plan to discover the splits to read:

```c++
// scan
paimon::ScanContextBuilder scan_context_builder(table_path);
PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::ScanContext> scan_context,
                        scan_context_builder.SetOptions(options).Finish());
PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::TableScan> scanner,
                        paimon::TableScan::Create(std::move(scan_context)));
PAIMON_ASSIGN_OR_RAISE(std::shared_ptr<paimon::Plan> plan, scanner->CreatePlan());
auto splits = plan->Splits();
```

Pass the planned splits to a table reader to obtain Arrow batches.

### Read Apache Arrow

Import each returned batch into Arrow C++ objects. This example collects the batches in memory;
for large results, process batches as they arrive instead.

```c++
PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::ReadContext> read_context,
                        read_context_builder.SetOptions(options).Finish());
PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::TableRead> table_read,
                        paimon::TableRead::Create(std::move(read_context)));
PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::BatchReader> batch_reader,
                        table_read->CreateReader(splits));
arrow::ArrayVector result_array_vector;
while (true) {
    PAIMON_ASSIGN_OR_RAISE(paimon::BatchReader::ReadBatch batch, batch_reader->NextBatch());
    if (paimon::BatchReader::IsEofBatch(batch)) {
        break;
    }
    auto& [c_array, c_schema] = batch;
    auto arrow_result = arrow::ImportArray(c_array.get(), c_schema.get());
    if (!arrow_result.ok()) {
        return paimon::Status::Invalid(arrow_result.status().ToString());
    }
    auto result_array = arrow_result.ValueUnsafe();
    result_array_vector.push_back(result_array);
}
auto chunk_result = arrow::ChunkedArray::Make(result_array_vector);
if (!chunk_result.ok()) {
    return paimon::Status::Invalid(chunk_result.status().ToString());
}
```

## Documentation

For more information, see [C++ Paimon Documentation](https://paimon.apache.org/docs/cpp/index.html).
