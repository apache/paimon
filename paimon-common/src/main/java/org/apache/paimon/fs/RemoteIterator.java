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

package org.apache.paimon.fs;

import java.io.Closeable;
import java.io.IOException;

/** An iterator for lazily listing remote entries. */
public interface RemoteIterator<E> extends Closeable {

    /**
     * Checks if there are more entries to be iterated.
     *
     * @return whether there are more elements to be iterated
     * @throws IOException - if failed to list entries lazily
     */
    boolean hasNext() throws IOException;

    /**
     * Gets the next entry to be iterated.
     *
     * @return the next entry
     * @throws IOException - if failed to list entries lazily
     */
    E next() throws IOException;
}
