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

package org.apache.paimon.globalindex.btree;

import org.apache.paimon.globalindex.GlobalIndexIOMeta;
import org.apache.paimon.globalindex.GlobalIndexReader;
import org.apache.paimon.testutils.junit.parameterized.ParameterizedTestExtension;

import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;

/** Test for {@link BTreeIndexReader} to read a single file. */
@ExtendWith(ParameterizedTestExtension.class)
public class BTreeIndexReaderTest extends AbstractIndexReaderTest {

    public BTreeIndexReaderTest(List<Object> args) {
        super(args);
    }

    @Override
    protected GlobalIndexReader prepareDataAndCreateReader() throws Exception {
        GlobalIndexIOMeta written = writeData(data);

        return new BTreeIndexReader(keySerializer, fileReader, written, CACHE_MANAGER);
    }
}
