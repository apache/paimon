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

package org.apache.paimon.security;

import org.apache.paimon.annotation.Public;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.HadoopUtils;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

/**
 * Security context that provides security module install and holds the security context.
 *
 * @since 0.4.0
 */
@Public
public class SecurityContext {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityContext.class);

    private static HadoopSecurityContext installedContext;

    /** Installs security configuration by {@link Options}. */
    public static void install(Options options) throws Exception {
        install(options, HadoopUtils.getHadoopConfiguration(options));
    }

    /** Installs security configuration by {@link CatalogContext}. */
    public static void install(CatalogContext catalogContext) throws Exception {
        install(catalogContext.options(), catalogContext.hadoopConf());
    }

    private static void install(Options options, Configuration configuration) throws Exception {
        SecurityConfiguration config = new SecurityConfiguration(options);
        if (config.isLegal()) {
            HadoopModule module = new HadoopModule(config, configuration);
            module.install();
            installedContext = new HadoopSecurityContext();
        }
    }

    /** Run with installed context. */
    public static <T> T runSecured(final Callable<T> securedCallable) throws Exception {
        return installedContext != null
                ? installedContext.runSecured(securedCallable)
                : securedCallable.call();
    }
}
