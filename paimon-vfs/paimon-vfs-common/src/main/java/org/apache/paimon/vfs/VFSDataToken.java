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

package org.apache.paimon.vfs;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/** Data token. */
public class VFSDataToken implements Serializable {
    private static final long serialVersionUID = 1L;
    private final Map<String, String> token;
    private final long expireAtMillis;
    @Nullable private Integer hash;

    public VFSDataToken(Map<String, String> token, long expireAtMillis) {
        this.token = token;
        this.expireAtMillis = expireAtMillis;
    }

    public Map<String, String> token() {
        return this.token;
    }

    public long expireAtMillis() {
        return this.expireAtMillis;
    }

    public boolean equals(Object o) {
        if (o != null && this.getClass() == o.getClass()) {
            VFSDataToken token1 = (VFSDataToken) o;
            return this.expireAtMillis == token1.expireAtMillis
                    && Objects.equals(this.token, token1.token);
        } else {
            return false;
        }
    }

    public int hashCode() {
        if (this.hash == null) {
            this.hash = Objects.hash(new Object[] {this.token, this.expireAtMillis});
        }

        return this.hash;
    }
}
