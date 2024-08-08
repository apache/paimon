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

package org.apache.paimon.flink.sink;

import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;

import java.io.Serializable;

/** Processor interface for {@link RecordAttributes}. */
public interface RecordAttributesProcessor extends Serializable {
    void processRecordAttributes(
            RecordAttributes recordAttributes, RecordAttributesContext context);

    /**
     * Context class for {@link RecordAttributesProcessor}, providing information to be used when
     * processing record attributes.
     */
    class RecordAttributesContext {
        private final StoreSinkWrite write;

        protected RecordAttributesContext(StoreSinkWrite write) {
            this.write = write;
        }

        public StoreSinkWrite getWrite() {
            return write;
        }
    }
}
