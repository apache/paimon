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

package org.apache.paimon.lookup;

import java.io.IOException;
import java.util.List;

/**
 * {@link State} interface for set state in Operations, the values must be sorted by byte array to
 * be returned.
 */
public interface SetState<K, V> extends State<K, V> {

    List<V> get(K key) throws IOException;

    void retract(K key, V value) throws IOException;

    void add(K key, V value) throws IOException;
}
