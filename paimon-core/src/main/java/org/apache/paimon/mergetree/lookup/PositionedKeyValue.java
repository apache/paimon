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

package org.apache.paimon.mergetree.lookup;

import org.apache.paimon.KeyValue;

/** {@link KeyValue} with file name and row position for DeletionVector. */
public class PositionedKeyValue {

    private final KeyValue keyValue;
    private final String fileName;
    private final long rowPosition;

    public PositionedKeyValue(KeyValue keyValue, String fileName, long rowPosition) {
        this.keyValue = keyValue;
        this.fileName = fileName;
        this.rowPosition = rowPosition;
    }

    public String fileName() {
        return fileName;
    }

    public long rowPosition() {
        return rowPosition;
    }

    public KeyValue keyValue() {
        return keyValue;
    }
}
