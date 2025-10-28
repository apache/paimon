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

package org.apache.paimon.mergetree.compact;

import org.apache.paimon.KeyValue;

/** For Postpone writer to validate retract records advance. */
public interface MergeFunctionPreValidator {

    void validate(KeyValue kv);

    /** Used for postpone bucket tables. */
    class PostponeValidator implements MergeFunctionPreValidator {

        private final MergeFunction<KeyValue> mergeFunction;

        private boolean retractValidated = false;

        public PostponeValidator(MergeFunction<KeyValue> mergeFunction) {
            this.mergeFunction = mergeFunction;
        }

        @Override
        public void validate(KeyValue kv) {
            if (kv.valueKind().isRetract()) {
                if (retractValidated) {
                    return;
                }
                mergeFunction.reset();
                mergeFunction.add(kv);
                mergeFunction.getResult();
                retractValidated = true;
            }
        }
    }

    /** Used for other tables. */
    class NoopValidator implements MergeFunctionPreValidator {

        @Override
        public void validate(KeyValue kv) {}
    }
}
