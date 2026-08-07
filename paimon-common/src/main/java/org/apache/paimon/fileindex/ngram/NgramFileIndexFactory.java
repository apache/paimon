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

package org.apache.paimon.fileindex.ngram;

import org.apache.paimon.fileindex.FileIndexer;
import org.apache.paimon.fileindex.FileIndexerFactory;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.CharType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.VarCharType;

/** Factory to create N-gram file index. */
public class NgramFileIndexFactory implements FileIndexerFactory {

    public static final String NGRAM_INDEX = "ngram";

    @Override
    public String identifier() {
        return NGRAM_INDEX;
    }

    @Override
    public FileIndexer create(DataType dataType, Options options) {
        if (!isStringType(dataType)) {
            throw new IllegalArgumentException(
                    "N-gram index only supports string types (VARCHAR, CHAR), got: " + dataType);
        }
        return new NgramFileIndex(options);
    }

    private boolean isStringType(DataType dataType) {
        return dataType instanceof VarCharType || dataType instanceof CharType;
    }
}
