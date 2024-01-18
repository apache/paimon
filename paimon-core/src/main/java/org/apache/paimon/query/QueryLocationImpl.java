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

package org.apache.paimon.query;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.service.ServiceManager;

import java.net.InetSocketAddress;
import java.util.Optional;

import static org.apache.paimon.service.ServiceManager.PRIMARY_KEY_LOOKUP;
import static org.apache.paimon.table.sink.ChannelComputer.select;

/** An implementation of {@link QueryLocation} to get location from {@link ServiceManager}. */
public class QueryLocationImpl implements QueryLocation {

    private final ServiceManager manager;

    private InetSocketAddress[] addressesCache;

    public QueryLocationImpl(ServiceManager manager) {
        this.manager = manager;
    }

    @Override
    public InetSocketAddress getLocation(BinaryRow partition, int bucket, boolean forceUpdate) {
        if (addressesCache == null || forceUpdate) {
            Optional<InetSocketAddress[]> addresses = manager.service(PRIMARY_KEY_LOOKUP);
            if (!addresses.isPresent()) {
                throw new RuntimeException(
                        "Cannot find address for table path: " + manager.tablePath());
            }
            addressesCache = addresses.get();
        }

        return addressesCache[select(partition, bucket, addressesCache.length)];
    }
}
