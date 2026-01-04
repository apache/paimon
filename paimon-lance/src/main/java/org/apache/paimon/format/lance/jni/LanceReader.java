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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Wrapper for Native Lance Reader. */
public class LanceReader {

    private static final Logger LOG = LoggerFactory.getLogger(LanceReader.class);

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
        } catch (IOException | RuntimeException e) {
            // 异常时清理已分配的资源，防止内存泄漏
            closeQuietly();
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
        closeQuietly();
    }

    /**
     * 安全关闭所有资源，忽略关闭过程中的异常。
     * 用于构造函数异常时的资源清理，以及正常关闭流程。
     */
    private void closeQuietly() {
        if (arrowReader != null) {
            try {
                arrowReader.close();
            } catch (Exception e) {
                LOG.warn("Failed to close arrowReader", e);
            }
            this.arrowReader = null;
        }

        if (reader != null) {
            try {
                reader.close();
            } catch (Exception e) {
                LOG.warn("Failed to close reader", e);
            }
            this.reader = null;
        }

        if (rootAllocator != null) {
            try {
                rootAllocator.close();
            } catch (Exception e) {
                LOG.warn("Failed to close rootAllocator", e);
            }
            this.rootAllocator = null;
        }
    }
}
