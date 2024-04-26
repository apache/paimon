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

package org.apache.paimon.lookup;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.EnvOptions;
import org.rocksdb.IngestExternalFileOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileWriter;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Bulk loader for RocksDB. */
public class BulkLoader {

    private final String uuid = UUID.randomUUID().toString();

    private final ColumnFamilyHandle columnFamily;
    private final String path;
    private final RocksDB db;
    private final Options options;
    private final List<String> files = new ArrayList<>();

    private SstFileWriter writer = null;
    private int sstIndex = 0;
    private long recordNum = 0;

    public BulkLoader(RocksDB db, Options options, ColumnFamilyHandle columnFamily, String path) {
        this.db = db;
        this.options = options;
        this.columnFamily = columnFamily;
        this.path = path;
    }

    public void write(byte[] key, byte[] value) throws WriteException {
        try {
            if (writer == null) {
                writer = new SstFileWriter(new EnvOptions(), options);
                String path = new File(this.path, "sst-" + uuid + "-" + (sstIndex++)).getPath();
                writer.open(path);
                files.add(path);
            }

            try {
                writer.put(key, value);
            } catch (RocksDBException e) {
                throw new WriteException(e);
            }

            recordNum++;
            if (recordNum % 1000 == 0 && writer.fileSize() >= options.targetFileSizeBase()) {
                writer.finish();
                writer.close();
                writer = null;
                recordNum = 0;
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    public void finish() {
        try {
            if (writer != null) {
                writer.finish();
                writer.close();
            }

            if (files.size() > 0) {
                IngestExternalFileOptions ingestOptions = new IngestExternalFileOptions();
                db.ingestExternalFile(columnFamily, files, ingestOptions);
                ingestOptions.close();
            }
        } catch (RocksDBException e) {
            throw new RuntimeException(e);
        }
    }

    /** Exception during writing. */
    public static class WriteException extends Exception {
        public WriteException(Throwable cause) {
            super(cause);
        }
    }
}
