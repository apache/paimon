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

package org.apache.paimon.jindo;

import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.oss.OSSBlobPresigner;
import org.apache.paimon.utils.StringUtils;

import com.aliyun.oss.OSSClient;
import com.aliyun.oss.OSSClientBuilder;

import java.io.IOException;
import java.time.Duration;

/** OSS presigning implementation loaded from the private plugin directory. */
public class JindoBlobPresigner implements JindoFileIO.BlobPresigner {

    private OSSClient client;

    public JindoBlobPresigner() {}

    JindoBlobPresigner(OSSClient client) {
        this.client = client;
    }

    @Override
    public void configure(Options options) {
        client = createBlobClient(options);
    }

    @Override
    public String create(Path tableRoot, BlobDescriptor descriptor, Duration validity)
            throws IOException {
        return OSSBlobPresigner.create(client, tableRoot, descriptor, validity);
    }

    @Override
    public void close() {
        client.shutdown();
    }

    static OSSClient createBlobClient(Options options) {
        String endpoint = options.get("fs.oss.endpoint");
        if (!endpoint.contains("://")) {
            endpoint = "https://" + endpoint;
        }
        String securityToken = options.get("fs.oss.securityToken");
        OSSClientBuilder builder = new OSSClientBuilder();
        OSSClient client =
                (OSSClient)
                        (StringUtils.isNullOrWhitespaceOnly(securityToken)
                                ? builder.build(
                                        endpoint,
                                        options.get("fs.oss.accessKeyId"),
                                        options.get("fs.oss.accessKeySecret"))
                                : builder.build(
                                        endpoint,
                                        options.get("fs.oss.accessKeyId"),
                                        options.get("fs.oss.accessKeySecret"),
                                        securityToken));
        String region = options.get("fs.oss.region");
        if (!StringUtils.isNullOrWhitespaceOnly(region)) {
            client.setRegion(region);
        }
        return client;
    }
}
