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

package org.apache.paimon.crosspartition;

/** Type of record, key or full row. */
public enum KeyPartOrRow {
    KEY_PART,
    ROW;

    public byte toByteValue() {
        switch (this) {
            case KEY_PART:
                return 0;
            case ROW:
                return 1;
            default:
                throw new UnsupportedOperationException("Unsupported value: " + this);
        }
    }

    public static KeyPartOrRow fromByteValue(byte value) {
        switch (value) {
            case 0:
                return KEY_PART;
            case 1:
                return ROW;
            default:
                throw new UnsupportedOperationException(
                        "Unsupported byte value '" + value + "' for row kind.");
        }
    }
}
