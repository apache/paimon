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

package org.apache.paimon.rest.responses;

import org.apache.paimon.rest.RESTResponse;

import java.util.List;

/** Response for altering database. */
public class AlertDatabaseResponse implements RESTResponse {

    // List of namespace property keys that were removed
    private List<String> removed;
    // List of namespace property keys that were added or updated
    private List<String> updated;
    // List of properties that were requested for removal that were not found in the namespace's
    // properties
    private List<String> missing;

    public AlertDatabaseResponse(List<String> removed, List<String> updated, List<String> missing) {
        this.removed = removed;
        this.updated = updated;
        this.missing = missing;
    }

    public List<String> getRemoved() {
        return removed;
    }

    public List<String> getUpdated() {
        return updated;
    }

    public List<String> getMissing() {
        return missing;
    }
}
