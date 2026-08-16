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

package org.apache.paimon.format;

import java.io.IOException;
import java.util.Map;

/** Format capability for recovering top-level field metadata from a file footer or schema. */
public interface SupportsFieldMetadata {

    /**
     * Reads metadata from top-level file fields.
     *
     * <p>The returned map is keyed by field name. Each value contains the metadata key-value pairs
     * attached to that field. Implementations return an empty map when the file does not contain
     * field metadata.
     */
    Map<String, Map<String, String>> readFieldMetadata(FormatReaderFactory.Context context)
            throws IOException;
}
