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

package org.apache.paimon.rest;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/** Token for REST Catalog to access file io. */
public class RESTToken implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<String, String> token;
    private final long expireAtMillis;

    /** Cache the hash code. */
    @Nullable private Integer hash;

    public RESTToken(Map<String, String> token, long expireAtMillis) {
        this.token = token;
        this.expireAtMillis = expireAtMillis;
    }

    public Map<String, String> token() {
        return token;
    }

    public long expireAtMillis() {
        return expireAtMillis;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RESTToken token1 = (RESTToken) o;
        return expireAtMillis == token1.expireAtMillis && Objects.equals(token, token1.token);
    }

    @Override
    public int hashCode() {
        if (hash == null) {
            hash = Objects.hash(token, expireAtMillis);
        }
        return hash;
    }
}
