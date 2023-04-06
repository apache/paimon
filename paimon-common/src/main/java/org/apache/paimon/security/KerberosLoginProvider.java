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

import org.apache.paimon.options.Options;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** Provides Kerberos login functionality. */
public class KerberosLoginProvider {

    private static final Logger LOG = LoggerFactory.getLogger(KerberosLoginProvider.class);

    private final String principal;

    private final String keytab;

    private final boolean useTicketCache;

    public KerberosLoginProvider(Options options) {
        checkNotNull(options, "options must not be null");
        SecurityConfiguration securityConfiguration = new SecurityConfiguration(options);
        this.principal = securityConfiguration.getPrincipal();
        this.keytab = securityConfiguration.getKeytab();
        this.useTicketCache = securityConfiguration.useTicketCache();
    }

    public KerberosLoginProvider(SecurityConfiguration config) {
        checkNotNull(config, "SecurityConfiguration must not be null");
        this.principal = config.getPrincipal();
        this.keytab = config.getKeytab();
        this.useTicketCache = config.useTicketCache();
    }

    public boolean isLoginPossible() throws IOException {
        if (UserGroupInformation.isSecurityEnabled()) {
            LOG.debug("Security is enabled");
        } else {
            LOG.debug("Security is NOT enabled");
            return false;
        }

        UserGroupInformation currentUser = UserGroupInformation.getCurrentUser();

        if (principal != null) {
            LOG.debug("Login from keytab is possible");
            return true;
        } else if (!isProxyUser(currentUser)) {
            if (useTicketCache && currentUser.hasKerberosCredentials()) {
                LOG.debug("Login from ticket cache is possible");
                return true;
            }
        } else {
            throwProxyUserNotSupported();
        }

        LOG.debug("Login is NOT possible");

        return false;
    }

    /**
     * Does kerberos login and sets current user. Must be called when isLoginPossible returns true.
     */
    public void doLogin() throws IOException {
        if (principal != null) {
            LOG.info(
                    "Attempting to login to KDC using principal: {} keytab: {}", principal, keytab);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
            LOG.info("Successfully logged into KDC");
        } else if (!isProxyUser(UserGroupInformation.getCurrentUser())) {
            LOG.info("Attempting to load user's ticket cache");
            UserGroupInformation.loginUserFromSubject(null);
            LOG.info("Loaded user's ticket cache successfully");
        } else {
            throwProxyUserNotSupported();
        }
    }

    private void throwProxyUserNotSupported() {
        throw new UnsupportedOperationException("Proxy user is not supported");
    }

    public static boolean isProxyUser(UserGroupInformation ugi) {
        return ugi.getAuthenticationMethod() == UserGroupInformation.AuthenticationMethod.PROXY;
    }

    public static boolean hasUserKerberosAuthMethod(UserGroupInformation ugi) {
        return UserGroupInformation.isSecurityEnabled()
                && ugi.getAuthenticationMethod()
                        == UserGroupInformation.AuthenticationMethod.KERBEROS;
    }
}
