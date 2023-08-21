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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.MockedStatic;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.apache.paimon.security.SecurityConfiguration.KERBEROS_LOGIN_KEYTAB;
import static org.apache.paimon.security.SecurityConfiguration.KERBEROS_LOGIN_PRINCIPAL;
import static org.apache.paimon.security.SecurityConfiguration.KERBEROS_LOGIN_USETICKETCACHE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Test for {@link KerberosLoginProvider}.
 *
 * <p>This class is an ITCase because the mocking breaks the {@link UserGroupInformation} class for
 * other tests.
 */
public class KerberosLoginProviderITCase {

    @Test
    public void isLoginPossibleMustReturnFalseByDefault() throws IOException {
        Options options = new Options();
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(options);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            assertThat(kerberosLoginProvider.isLoginPossible()).isFalse();
        }
    }

    @Test
    public void isLoginPossibleMustReturnFalseWithNonKerberos() throws IOException {
        Options options = new Options();
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(options);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            ugi.when(UserGroupInformation::isSecurityEnabled).thenReturn(false);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            assertThat(kerberosLoginProvider.isLoginPossible()).isFalse();
        }
    }

    @Test
    public void isLoginPossibleMustReturnTrueWithKeytab(@TempDir Path tmpDir) throws IOException {
        Options options = new Options();
        options.set(KERBEROS_LOGIN_PRINCIPAL, "principal");
        final Path keyTab = Files.createFile(tmpDir.resolve("test.keytab"));
        options.set(KERBEROS_LOGIN_KEYTAB, keyTab.toAbsolutePath().toString());
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(options);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            ugi.when(UserGroupInformation::isSecurityEnabled).thenReturn(true);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            assertThat(kerberosLoginProvider.isLoginPossible()).isTrue();
        }
    }

    @Test
    public void isLoginPossibleMustReturnTrueWithTGT() throws IOException {
        Options options = new Options();
        options.set(KERBEROS_LOGIN_USETICKETCACHE, true);
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(options);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            when(userGroupInformation.hasKerberosCredentials()).thenReturn(true);
            ugi.when(UserGroupInformation::isSecurityEnabled).thenReturn(true);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            assertThat(kerberosLoginProvider.isLoginPossible()).isTrue();
        }
    }

    @Test
    public void isLoginPossibleMustThrowExceptionWithProxyUser() {
        Options options = new Options();
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(options);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            when(userGroupInformation.getAuthenticationMethod())
                    .thenReturn(UserGroupInformation.AuthenticationMethod.PROXY);
            ugi.when(UserGroupInformation::isSecurityEnabled).thenReturn(true);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            assertThatThrownBy(kerberosLoginProvider::isLoginPossible)
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }

    @Test
    public void doLoginMustLoginWithKeytab(@TempDir Path tmpDir) throws IOException {
        Options options = new Options();
        options.set(KERBEROS_LOGIN_PRINCIPAL, "principal");
        final Path keyTab = Files.createFile(tmpDir.resolve("test.keytab"));
        options.set(KERBEROS_LOGIN_KEYTAB, keyTab.toAbsolutePath().toString());
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(options);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            kerberosLoginProvider.doLogin();
            ugi.verify(() -> UserGroupInformation.loginUserFromKeytab(anyString(), anyString()));
        }
    }

    @Test
    public void doLoginMustLoginWithTGT() throws IOException {
        Options options = new Options();
        options.set(KERBEROS_LOGIN_USETICKETCACHE, true);
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(options);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            when(userGroupInformation.hasKerberosCredentials()).thenReturn(true);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            kerberosLoginProvider.doLogin();
            ugi.verify(() -> UserGroupInformation.loginUserFromSubject(null));
        }
    }

    @Test
    public void doLoginMustThrowExceptionWithProxyUser() {
        Options options = new Options();
        KerberosLoginProvider kerberosLoginProvider = new KerberosLoginProvider(options);

        try (MockedStatic<UserGroupInformation> ugi = mockStatic(UserGroupInformation.class)) {
            UserGroupInformation userGroupInformation = mock(UserGroupInformation.class);
            when(userGroupInformation.getAuthenticationMethod())
                    .thenReturn(UserGroupInformation.AuthenticationMethod.PROXY);
            ugi.when(UserGroupInformation::getCurrentUser).thenReturn(userGroupInformation);

            assertThatThrownBy(kerberosLoginProvider::doLogin)
                    .isInstanceOf(UnsupportedOperationException.class);
        }
    }
}
