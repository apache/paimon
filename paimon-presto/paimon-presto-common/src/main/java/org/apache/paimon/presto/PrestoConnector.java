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

package org.apache.paimon.presto;

import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorCommitHandle;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.connector.EmptyConnectorCommitHandle;

/** Presto {@link Connector}. */
public class PrestoConnector extends PrestoConnectorBase {

    private final PrestoTransactionManager transactionManager;

    public PrestoConnector(
            PrestoTransactionManager transactionManager,
            PrestoSplitManager prestoSplitManager,
            PrestoPageSourceProvider prestoPageSourceProvider,
            PrestoMetadataFactory prestoMetadataFactory) {
        super(
                transactionManager,
                prestoSplitManager,
                prestoPageSourceProvider,
                prestoMetadataFactory);
        this.transactionManager = transactionManager;
    }

    @Override
    public ConnectorCommitHandle commit(ConnectorTransactionHandle transaction) {
        transactionManager.remove(transaction);
        return EmptyConnectorCommitHandle.INSTANCE;
    }
}
