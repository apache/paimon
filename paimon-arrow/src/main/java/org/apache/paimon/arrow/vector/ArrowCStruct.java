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

package org.apache.paimon.arrow.vector;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

/** Cache for Arrow c struct. */
public class ArrowCStruct {

    private final ArrowArray array;
    private final ArrowSchema schema;

    public ArrowCStruct(ArrowArray array, ArrowSchema schema) {
        this.array = array;
        this.schema = schema;
    }

    public long arrayAddress() {
        return array.memoryAddress();
    }

    public long schemaAddress() {
        return schema.memoryAddress();
    }

    public static ArrowCStruct of(ArrowArray array, ArrowSchema schema) {
        return new ArrowCStruct(array, schema);
    }
}
