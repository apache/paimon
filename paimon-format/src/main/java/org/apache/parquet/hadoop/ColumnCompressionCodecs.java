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

package org.apache.parquet.hadoop;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Compression codecs configured per parquet column path. */
class ColumnCompressionCodecs {

    private final CompressionCodecName defaultCodec;
    private final Map<ColumnPath, CompressionCodecName> columnCodecs;

    ColumnCompressionCodecs(CompressionCodecName defaultCodec) {
        this(defaultCodec, Collections.<ColumnPath, CompressionCodecName>emptyMap());
    }

    ColumnCompressionCodecs(
            CompressionCodecName defaultCodec, Map<ColumnPath, CompressionCodecName> columnCodecs) {
        this.defaultCodec = defaultCodec;
        this.columnCodecs =
                columnCodecs.isEmpty()
                        ? Collections.<ColumnPath, CompressionCodecName>emptyMap()
                        : new HashMap<>(columnCodecs);
    }

    CompressionCodecName getCodec(ColumnDescriptor descriptor) {
        CompressionCodecName codec = columnCodecs.get(ColumnPath.get(descriptor.getPath()));
        return codec == null ? defaultCodec : codec;
    }
}
