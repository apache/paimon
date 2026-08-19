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

/** Helpers for savepoint tags — the tags Paimon auto-creates to mark Flink savepoints. */
public class SavepointTagUtils {

    /** Prefix shared by every savepoint auto-tag; use {@link #tagNameOf(long)} for a full name. */
    public static final String PREFIX = "savepoint-";

    /** Name of the auto-tag for a savepoint committed under {@code commitIdentifier}. */
    public static String tagNameOf(long commitIdentifier) {
        return PREFIX + commitIdentifier;
    }
}
