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

package org.apache.paimon.vfs.hadoop;

import org.apache.paimon.rest.auth.AuthProviderEnum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;

/** Test for {@link PaimonVirtualFileSystem} with Mock Rest Server and disable table cache. */
public class MockRestNoCacheVirtualFileSystemTest extends MockRestVirtualFileSystemTest {

    @Override
    protected void initFs() throws Exception {
        Configuration conf = new Configuration();
        // With inline endpoint in uri
        String endpoint = restCatalogServer.getUrl();
        if (endpoint.startsWith("http://")) {
            endpoint = endpoint.substring("http://".length());
        }
        if (endpoint.endsWith("/")) {
            endpoint = endpoint.substring(0, endpoint.length() - 1);
        }

        conf.set("fs.pvfs.token.provider", AuthProviderEnum.BEAR.identifier());
        conf.set("fs.pvfs.token", initToken);
        // Disable table cache
        conf.setBoolean("fs.pvfs.cache-enabled", false);
        this.vfs = new PaimonVirtualFileSystem();
        this.vfsRoot = new Path("pvfs://" + restWarehouse + "." + endpoint + "/");
        this.vfs.initialize(vfsRoot.toUri(), conf);

        Assert.assertFalse(((PaimonVirtualFileSystem) vfs).isCacheEnabled());
    }
}
