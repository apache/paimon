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
import java.util.function.Supplier;

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
    @Deprecated
    public static void install(Options options) throws Exception {
        install(options, () -> HadoopUtils.getHadoopConfiguration(options));
    }

    /** Installs security configuration by {@link CatalogContext}. */
    public static void install(CatalogContext catalogContext) throws Exception {
        install(catalogContext.options(), catalogContext::hadoopConf);
    }

    private static void install(Options options, Supplier<Configuration> configurationSupplier)
            throws Exception {
        SecurityConfiguration config = new SecurityConfiguration(options);
        if (config.isLegal()) {
            HadoopModule module = createModule(config, configurationSupplier);
            if (module != null) {
                module.install();
                installedContext = new HadoopSecurityContext();
            }
        }
    }

    /** Run with installed context. */
    public static <T> T runSecured(final Callable<T> securedCallable) throws Exception {
        return installedContext != null
                ? installedContext.runSecured(securedCallable)
                : securedCallable.call();
    }

    private static HadoopModule createModule(
            SecurityConfiguration securityConfig, Supplier<Configuration> configurationSupplier) {
        // First check if we have Hadoop in the ClassPath. If not, we simply don't do anything.
        if (!isHadoopCommonOnClasspath(HadoopModule.class.getClassLoader())) {
            LOG.info(
                    "Cannot create Hadoop Security Module because Hadoop cannot be found in the Classpath.");
            return null;
        }

        try {
            return new HadoopModule(securityConfig, configurationSupplier.get());
        } catch (LinkageError e) {
            LOG.warn(
                    "Cannot create Hadoop Security Module due to an error that happened while instantiating the module. No security module will be loaded.",
                    e);
            return null;
        }
    }

    public static boolean isHadoopCommonOnClasspath(ClassLoader classLoader) {
        try {
            LOG.debug("Checking whether hadoop common dependency in on classpath.");
            Class.forName("org.apache.hadoop.conf.Configuration", false, classLoader);
            Class.forName("org.apache.hadoop.security.UserGroupInformation", false, classLoader);
            LOG.debug("Hadoop common dependency found on classpath.");
            return true;
        } catch (ClassNotFoundException e) {
            LOG.debug("Hadoop common dependency cannot be found on classpath.");
            return false;
        }
    }
}
