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

import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.SensitiveConfigUtils;

import com.aliyun.jindodata.oss.auth.SimpleCredentialsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/** Jindo {@link JindoHadoopOptions}. */
public class JindoHadoopOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(JindoHadoopOptions.class);

    private Options hadoopOptions;
    private Options hadoopOptionsWithCache;

    /**
     * In order to simplify, we make paimon oss configuration keys same with hadoop oss module. So,
     * we add all configuration key with prefix `fs.` in paimon conf to hadoop conf.
     *
     * <p>Not use fs.oss because this FileIO also access dlf/dls scheme.
     */
    private static final String[] CONFIG_PREFIXES = {"fs."};

    private static final String OSS_ACCESS_KEY_ID = "fs.oss.accessKeyId";
    private static final String OSS_ACCESS_KEY_SECRET = "fs.oss.accessKeySecret";
    private static final String OSS_SECURITY_TOKEN = "fs.oss.securityToken";
    private static final String OSS_SHOW_DIR_TIMESTAMP = "fs.oss.show-dir-timestamp";

    private static final Map<String, String> CASE_SENSITIVE_KEYS =
            new HashMap<String, String>() {
                {
                    put(OSS_ACCESS_KEY_ID.toLowerCase(Locale.ROOT), OSS_ACCESS_KEY_ID);
                    put(OSS_ACCESS_KEY_SECRET.toLowerCase(Locale.ROOT), OSS_ACCESS_KEY_SECRET);
                    put(OSS_SECURITY_TOKEN.toLowerCase(Locale.ROOT), OSS_SECURITY_TOKEN);
                }
            };

    private boolean isJindoCacheBackend(CatalogContext context) {
        return context.options().get("fs.jindocache.namespace.rpc.address") != null;
    }

    public JindoHadoopOptions(CatalogContext context) {
        hadoopOptions = new Options();
        // read all configuration with prefix 'CONFIG_PREFIXES'
        for (String key : context.options().keySet()) {
            for (String prefix : CONFIG_PREFIXES) {
                if (key.startsWith(prefix)) {
                    String value = context.options().get(key);
                    if (CASE_SENSITIVE_KEYS.containsKey(key.toLowerCase(Locale.ROOT))) {
                        key = CASE_SENSITIVE_KEYS.get(key.toLowerCase(Locale.ROOT));
                    }
                    hadoopOptions.set(key, value);

                    LOG.debug(
                            "Adding config entry for {} as {} to Hadoop config",
                            key,
                            SensitiveConfigUtils.redactValue(key, hadoopOptions.get(key)));
                }
            }
        }
        // as in rest catalog use could define ak for table so we need first use ak.
        if (hadoopOptions.containsKey(OSS_ACCESS_KEY_ID)
                && hadoopOptions.containsKey(OSS_ACCESS_KEY_SECRET)) {
            LOG.info("Using Ak init Jindo.");
            // https://github.com/aliyun/alibabacloud-jindodata/blob/master/docs/user/4.x/4.6.x/4.6.1/oss/hadoop/jindosdk_ide_hadoop.md
            hadoopOptions.set("fs.oss.impl", "com.aliyun.jindodata.oss.JindoOssFileSystem");
            hadoopOptions.set("fs.AbstractFileSystem.oss.impl", "com.aliyun.jindodata.oss.OSS");

            // Misalignment can greatly affect performance, so the maximum buffer is set here
            hadoopOptions.set("fs.oss.read.position.buffer.size", "8388608");
            hadoopOptions.set("fs.oss.credentials.provider", SimpleCredentialsProvider.NAME);
        } else {
            LOG.info("Using hadoop conf init Jindo.");
            context.hadoopConf()
                    .iterator()
                    .forEachRemaining(entry -> hadoopOptions.set(entry.getKey(), entry.getValue()));
        }

        // Resolving a timestamp for every listed directory is expensive in Jindo.
        if (!hadoopOptions.containsKey(OSS_SHOW_DIR_TIMESTAMP)) {
            hadoopOptions.set(OSS_SHOW_DIR_TIMESTAMP, "false");
        }

        JindoUserAgent.apply(context.options(), hadoopOptions);

        if (isJindoCacheBackend(context)) {
            initJindoCacheBackend(context);
        } else {
            initDLFCacheBackend(context);
        }
    }

    public Options get(boolean enableCache) {
        return enableCache ? hadoopOptionsWithCache : hadoopOptions;
    }

    private void initJindoCacheBackend(CatalogContext context) {
        // another config using jindocache
        hadoopOptionsWithCache = new Options(hadoopOptions.toMap());
        hadoopOptionsWithCache.set("fs.xengine", "jindocache");
        if (!hadoopOptionsWithCache.containsKey("fs.jindocache.client.metrics.enable")) {
            // enable metrics report by default
            hadoopOptionsWithCache.set("fs.jindocache.client.metrics.enable", "true");
        }
        // Workaround: following configurations to avoid bug in some JindoSDK versions
        hadoopOptionsWithCache.set("fs.oss.read.profile.columnar.use-pread", "false");
        hadoopOptionsWithCache.set(
                "fs.jindocache.read.profile.columnar.readahead.pread.enable", "false");
    }

    private void initDLFCacheBackend(CatalogContext context) {
        hadoopOptionsWithCache = new Options(hadoopOptions.toMap());
        // DLF cache provide a fallback OSS endpoint for direct access to OSS without cache
        if (context.options().get("fs.oss.direct.endpoint") != null) {
            hadoopOptions.set("fs.oss.endpoint", context.options().get("fs.oss.direct.endpoint"));
        }
        LOG.info(
                "Using direct access to OSS endpoint: {}, cache endpoint: {}",
                hadoopOptions.get("fs.oss.endpoint"),
                hadoopOptionsWithCache.get("fs.oss.endpoint"));
    }
}
