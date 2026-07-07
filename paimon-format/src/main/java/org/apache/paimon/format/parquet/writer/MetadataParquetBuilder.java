/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.parquet.writer;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/** A {@link ParquetBuilder} that can provide writer metadata before the file is finalized. */
public interface MetadataParquetBuilder<T> extends ParquetBuilder<T> {

    /**
     * Creates and configures a parquet writer that can read metadata before finalizing the file.
     */
    ParquetWriter<T> createWriter(
            OutputFile out, String compression, Supplier<Map<String, byte[]>> metadataSupplier)
            throws IOException;
}
