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

import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.StringUtils;

import java.io.File;

import static org.apache.paimon.options.ConfigOptions.key;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** The security configuration. */
public class SecurityConfiguration {

    public static final ConfigOption<String> KERBEROS_LOGIN_KEYTAB =
            key("security.kerberos.login.keytab")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("security.keytab")
                    .withDescription(
                            "Absolute path to a Kerberos keytab file that contains the user credentials.");

    public static final ConfigOption<String> KERBEROS_LOGIN_PRINCIPAL =
            key("security.kerberos.login.principal")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("security.principal")
                    .withDescription("Kerberos principal name associated with the keytab.");

    public static final ConfigOption<Boolean> KERBEROS_LOGIN_USETICKETCACHE =
            key("security.kerberos.login.use-ticket-cache")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription("Indicates whether to read from your Kerberos ticket cache.");

    private final Options options;

    private final boolean useTicketCache;

    private final String keytab;

    private final String principal;

    public SecurityConfiguration(Options options) {
        this.options = checkNotNull(options);
        this.keytab = options.get(KERBEROS_LOGIN_KEYTAB);
        this.principal = options.get(KERBEROS_LOGIN_PRINCIPAL);
        this.useTicketCache = options.get(KERBEROS_LOGIN_USETICKETCACHE);
    }

    public String getKeytab() {
        return keytab;
    }

    public String getPrincipal() {
        return principal;
    }

    public boolean useTicketCache() {
        return useTicketCache;
    }

    public Options getOptions() {
        return options;
    }

    public boolean isLegal() {
        if (StringUtils.isBlank(keytab) != StringUtils.isBlank(principal)) {
            return false;
        }

        if (!StringUtils.isBlank(keytab)) {
            File keytabFile = new File(keytab);
            return keytabFile.exists() && keytabFile.isFile() && keytabFile.canRead();
        }

        return true;
    }
}
