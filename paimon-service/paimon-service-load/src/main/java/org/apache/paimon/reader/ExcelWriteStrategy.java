/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.reader;

import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.sink.BatchTableWrite;

/**
 * Strategy implementation for writing data to Excel format using batch table writes.
 */
public class ExcelWriteStrategy implements WriteStrategy {

    @Override
    public void writer(BatchTableWrite batchTableWrite, String content, String columnSeparator)
            throws Exception {}

    @Override
    public Schema retrieveSchema() throws Exception {
        return null;
    }
}
