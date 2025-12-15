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

package org.apache.paimon.globalindex;

import org.apache.paimon.globalindex.io.GlobalIndexFileReader;
import org.apache.paimon.globalindex.io.GlobalIndexFileWriter;
import org.apache.paimon.options.Options;
import org.apache.paimon.types.DataField;

import java.io.IOException;
import java.util.List;

/** Abstract base class for global indexers. */
public interface GlobalIndexer {

    GlobalIndexWriter createWriter(GlobalIndexFileWriter fileWriter) throws IOException;

    GlobalIndexReader createReader(GlobalIndexFileReader fileReader, List<GlobalIndexIOMeta> files)
            throws IOException;

    static GlobalIndexer create(String type, DataField dataField, Options options) {
        GlobalIndexerFactory globalIndexerFactory = GlobalIndexerFactoryUtils.load(type);
        return globalIndexerFactory.create(dataField, options);
    }
}
