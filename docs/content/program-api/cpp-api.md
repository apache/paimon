---
title: "Cpp API"
weight: 6
type: docs
aliases:
  - /api/cpp-api.html
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

# Cpp API

Paimon C++ is a high-performance C++ implementation of Apache Paimon. Paimon C++ aims to provide a native,
high-performance and extensible implementation that allows native engines to access the Paimon datalake
format with maximum efficiency.

## Environment Settings

[Paimon C++](https://github.com/alibaba/paimon-cpp.git) is currently governed under Alibaba open source
community. You can checkout the [document](https://alibaba.github.io/paimon-cpp/getting_started.html)
for more details about envinroment settings.

```sh
git clone https://github.com/alibaba/paimon-cpp.git
cd paimon-cpp
mkdir build-release
cd build-release
cmake ..
make -j8       # if you have 8 CPU cores, otherwise adjust
make install
```

## Create Catalog

Before coming into contact with the Table, you need to create a Catalog.

```c++
#include "paimon/catalog/catalog.h"

// Note that keys and values are all string
std::map<std::string, std::string> options;
PAIMON_ASSIGN_OR_RAISE(std::unique_ptr<paimon::Catalog> catalog,
                       paimon::Catalog::Create(root_path, options));
```

Current C++ Paimon only supports filesystem catalog. In the future, we will support REST catalog.
See [Catalog]({{< ref "concepts/catalog" >}}).

You can use the catalog to create table for writing data.

## Create Database

Table is located in a database. If you want to create table in a new database, you should create it.

```c++
PAIMON_RETURN_NOT_OK(catalog->CreateDatabase('database_name', options, /*ignore_if_exists=*/false));
```

## Create Table

Table schema contains fields definition, partition keys, primary keys, table options.
The field definition is described by `Arrow::Schema`. All arguments except fields definition are optional.

for example:

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

See [Data Types](https://alibaba.github.io/paimon-cpp/user_guide/data_types.html) for all supported
`arrow-to-paimon` data types mapping.

## Batch Write

Paimon table write is Two-Phase Commit, you can write many times, but once committed, no more data can be written.
C++ Paimon uses Apache Arrow as [in-memory format], check out [document](https://alibaba.github.io/paimon-cpp/user_guide/arrow.html)
for more details.

for example:
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

### Predicate pushdown

A `ReadContextBuilder` is used to pass context to reader, push down and filter is done by reader.

```c++
ReadContextBuilder read_context_builder(table_path);
```

You can use `PredicateBuilder` to build filters and pushdown them by `ReadContextBuilder`:

```c++
# Example filter: 'f3' > 12.0 OR 'f1' == 1
PAIMON_ASSIGN_OR_RAISE(
    auto predicate,
    PredicateBuilder::Or(
        {PredicateBuilder::GreaterThan(/*field_index=*/3, /*field_name=*/"f3",
                                        FieldType::DOUBLE, Literal(static_cast<double>(12.0))),
        PredicateBuilder::Equal(/*field_index=*/1, /*field_name=*/"f1", FieldType::INT,
                                    Literal(1))}));
ReadContextBuilder read_context_builder(table_path);
read_context_builder.SetPredicate(predicate).EnablePredicateFilter(true);
```

You can also pushdown projection by `ReadContextBuilder`:

```c++
# select f3 and f2 columns
read_context_builder.SetReadSchema({"f3", "f1", "f2"});
```

### Generate Splits

Then you can step into Scan Plan stage to get `splits`:

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

Finally, you can read data from the `splits` to arrow format.

### Read Apache Arrow

This requires `C++ Arrow` to be installed.

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

For more information, See [C++ Paimon Documentation](https://alibaba.github.io/paimon-cpp/index.html).
