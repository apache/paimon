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

package org.apache.paimon.catalog;

import org.apache.paimon.factories.Factory;
import org.apache.paimon.factories.FactoryUtil;
import org.apache.paimon.operation.Lock;
import org.apache.paimon.options.Options;

import javax.annotation.Nullable;

import java.io.Serializable;

/** Factory to create table-bound {@link Lock} instances. */
public interface CatalogLockFactory extends Factory, Serializable {

    /**
     * Create a lock bound to the table and its branch. The UUID and writer identity may be null for
     * catalog operations; snapshot commits supply them along with the runtime table options.
     */
    Lock createLock(
            CatalogLockContext context,
            Identifier identifier,
            @Nullable String tableUuid,
            @Nullable String commitUser,
            Options tableOptions);

    static CatalogLockFactory discover(String identifier) {
        return FactoryUtil.discoverFactory(
                CatalogLockFactory.class.getClassLoader(), CatalogLockFactory.class, identifier);
    }
}
