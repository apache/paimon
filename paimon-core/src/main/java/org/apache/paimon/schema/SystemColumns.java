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

package org.apache.paimon.schema;

import java.util.Arrays;
import java.util.List;

/** System columns for key value store. */
public class SystemColumns {

    /** System field names. */
    public static final String KEY_FIELD_PREFIX = "_KEY_";

    public static final String VALUE_COUNT = "_VALUE_COUNT";
    public static final String SEQUENCE_NUMBER = "_SEQUENCE_NUMBER";
    public static final String VALUE_KIND = "_VALUE_KIND";
    public static final String LEVEL = "_LEVEL";
    public static final List<String> SYSTEM_FIELD_NAMES =
            Arrays.asList(VALUE_COUNT, SEQUENCE_NUMBER, VALUE_KIND);
}
