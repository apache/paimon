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

package org.apache.paimon.format.lance.jni;

import org.apache.paimon.types.RowType;

import com.lancedb.lance.file.LanceFileReader;
import com.lancedb.lance.util.Range;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import javax.annotation.Nullable;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/** Wrapper for Native Lance Reader. */
public class LanceReader {

    private RootAllocator rootAllocator;
    private LanceFileReader reader;
    private ArrowReader arrowReader;

    public LanceReader(
            String path,
            RowType projectedRowType,
            @Nullable List<Range> ranges,
            int batchSize,
            Map<String, String> storageOptions) {
        this.rootAllocator = new RootAllocator();
        try {
            this.reader = LanceFileReader.open(path, storageOptions, rootAllocator);
            this.arrowReader = reader.readAll(projectedRowType.getFieldNames(), ranges, batchSize);
        } catch (IOException e) {
            throw new RuntimeException("Failed to open Lance file: " + path, e);
        }
    }

    public VectorSchemaRoot readBatch() throws IOException {
        if (arrowReader.loadNextBatch()) {
            return arrowReader.getVectorSchemaRoot();
        } else {
            throw new EOFException("End of file reached");
        }
    }

    public void close() throws IOException {
        if (arrowReader != null) {
            arrowReader.close();
            this.arrowReader = null;
        }

        if (reader != null) {
            try {
                reader.close();
            } catch (Exception e) {
                throw new IOException(e);
            }
            this.reader = null;
        }

        if (rootAllocator != null) {
            rootAllocator.close();
            this.rootAllocator = null;
        }
    }
}
