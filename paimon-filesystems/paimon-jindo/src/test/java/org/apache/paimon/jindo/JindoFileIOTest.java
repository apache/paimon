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
import org.apache.paimon.data.BlobDescriptor;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.RenamingTwoPhaseOutputStream;
import org.apache.paimon.fs.TwoPhaseOutputStream;
import org.apache.paimon.options.Options;
import org.apache.paimon.utils.Pair;

import com.aliyun.jindodata.common.JindoHadoopSystem;
import com.aliyun.oss.HttpMethod;
import com.aliyun.oss.OSSClient;
import com.aliyun.oss.model.ObjectMetadata;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link JindoFileIO}. */
public class JindoFileIOTest {

    private static final String SHOW_DIR_TIMESTAMP = "fs.oss.show-dir-timestamp";

    private static final String PLUGIN_DIRECTORY = "paimon-plugin-jindo-oss/";

    private static final String PRESIGNER_IMPLEMENTATION =
            "org.apache.paimon.jindo.JindoBlobPresigner";

    @TempDir java.nio.file.Path tempDir;

    @Test
    public void testDisableDirectoryTimestampByDefault() {
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(new Options()));

        assertThat(
                        fileIO.hadoopOptions(new Path("oss://bucket/table"), "meta")
                                .get(SHOW_DIR_TIMESTAMP))
                .isEqualTo("false");
    }

    @Test
    public void testKeepExplicitDirectoryTimestampSetting() {
        Options options = new Options();
        options.set(SHOW_DIR_TIMESTAMP, "true");
        JindoFileIO fileIO = new JindoFileIO();
        fileIO.configure(CatalogContext.create(options));

        assertThat(
                        fileIO.hadoopOptions(new Path("oss://bucket/table"), "meta")
                                .get(SHOW_DIR_TIMESTAMP))
                .isEqualTo("true");
    }

    @Test
    public void testCreateBlobClientUsesConfiguredSts() {
        Options options = new Options();
        options.set("fs.oss.endpoint", "oss.example.com");
        options.set("fs.oss.region", "cn-hangzhou");
        options.set("fs.oss.accessKeyId", "access-key");
        options.set("fs.oss.accessKeySecret", "access-secret");
        options.set("fs.oss.securityToken", "security-token");

        OSSClient client = JindoBlobPresigner.createBlobClient(options);
        try {
            assertThat(client.getEndpoint()).isEqualTo(URI.create("https://oss.example.com"));
            assertThat(client.getObjectOperation().getRegion()).isEqualTo("cn-hangzhou");
            assertThat(client.getCredentialsProvider().getCredentials().getAccessKeyId())
                    .isEqualTo("access-key");
            assertThat(client.getCredentialsProvider().getCredentials().getSecretAccessKey())
                    .isEqualTo("access-secret");
            assertThat(client.getCredentialsProvider().getCredentials().getSecurityToken())
                    .isEqualTo("security-token");
            URL signedUrl =
                    client.generatePresignedUrl(
                            "bucket", "object", new Date(System.currentTimeMillis() + 60_000));
            assertThat(signedUrl.getProtocol()).isEqualTo("https");
            assertThat(signedUrl.getHost()).isEqualTo("bucket.oss.example.com");
            assertThat(signedUrl.getPath()).isEqualTo("/object");
            assertThat(signedUrl.getQuery())
                    .contains(
                            "OSSAccessKeyId=access-key",
                            "Signature=",
                            "security-token=security-token");
        } finally {
            client.shutdown();
        }
    }

    @Test
    public void testCreateBlobPresignedUrlUsesOssClient() throws Exception {
        OSSClient client = mock(OSSClient.class);
        Path tableRoot = new Path("oss://bucket/table");
        BlobDescriptor descriptor = new BlobDescriptor("oss://bucket/table/data/file", 10, 20);
        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(20);
        metadata.setContentType("application/octet-stream");
        metadata.addUserMetadata(
                "paimon-blob-descriptor-sha256", sha256Hex(descriptor.serialize()));
        when(client.headObject(eq("bucket"), contains("_bloburl_"))).thenReturn(metadata);
        when(client.getEndpoint()).thenReturn(URI.create("https://oss.example.com"));
        when(client.generatePresignedUrl(eq("bucket"), anyString(), any(), eq(HttpMethod.GET)))
                .thenAnswer(
                        invocation ->
                                new URL(
                                        "https://bucket.oss.example.com/"
                                                + invocation.getArgument(1)));

        JindoFileIO fileIO = new JindoFileIO(new JindoBlobPresigner(client));
        assertThat(fileIO.createBlobPresignedUrl(tableRoot, descriptor, Duration.ofHours(1)))
                .startsWith("https://bucket.oss.example.com/table/data/_bloburl_");

        fileIO.close();
        verify(client).shutdown();
    }

    @Test
    public void testFallbackToRenamingWhenMultipartUploadUnsupported() throws Exception {
        JindoHadoopSystem fs = mock(JindoHadoopSystem.class);
        org.apache.hadoop.fs.Path hadoopPath = mock(org.apache.hadoop.fs.Path.class);
        when(fs.exists(any())).thenReturn(false);
        when(fs.getMpuStore(any())).thenReturn(null);
        when(fs.create(any(), eq(false))).thenReturn(mock(FSDataOutputStream.class));

        JindoFileIO fileIO = new TestingJindoFileIO(fs, hadoopPath);
        TwoPhaseOutputStream stream =
                fileIO.newTwoPhaseOutputStream(
                        new Path("oss://bucket.cn-hangzhou.oss-dls.aliyuncs.com/table/file"),
                        false);

        assertThat(stream).isInstanceOf(RenamingTwoPhaseOutputStream.class);
        stream.close();
        verify(fs).getMpuStore(hadoopPath);
    }

    @Test
    @Tag("plugin-isolation")
    @EnabledIfSystemProperty(named = "jindo.plugin.jar", matches = ".+")
    public void testPluginJarContents() throws Exception {
        try (JarFile jar = new JarFile(pluginJar())) {
            Set<String> entries =
                    jar.stream()
                            .map(JarEntry::getName)
                            .map(entry -> entry.replaceFirst("^META-INF/versions/[0-9]+/", ""))
                            .collect(Collectors.toSet());
            for (String originalPackage :
                    Arrays.asList(
                            "com/google/gson/",
                            "com/aliyun/oss/",
                            "com/aliyuncs/",
                            "org/apache/http/",
                            "org/apache/commons/codec/",
                            "org/apache/commons/logging/",
                            "org/jdom2/",
                            "org/codehaus/jettison/",
                            "io/opentracing/",
                            "org/ini4j/",
                            "javax/xml/stream/")) {
                assertThat(entries)
                        .as("Private package %s at the root of %s", originalPackage, jar.getName())
                        .noneMatch(entry -> entry.startsWith(originalPackage));
            }
            for (String representativeClass :
                    Arrays.asList(
                            PRESIGNER_IMPLEMENTATION.replace('.', '/'),
                            "org/apache/paimon/oss/OSSBlobPresigner",
                            "com/google/gson/Gson",
                            "com/aliyun/oss/OSSClient",
                            "com/aliyuncs/DefaultAcsClient",
                            "org/apache/http/impl/client/HttpClients",
                            "org/apache/commons/codec/binary/Base64",
                            "org/jdom2/Document",
                            "org/codehaus/jettison/json/JSONObject",
                            "io/opentracing/Tracer",
                            "org/ini4j/Ini")) {
                assertThat(entries)
                        .contains(PLUGIN_DIRECTORY + representativeClass + ".class")
                        .doesNotContain(representativeClass + ".class");
            }
            for (String rootEntry :
                    Arrays.asList(
                            "org/apache/paimon/jindo/JindoFileIO.class",
                            "org/apache/paimon/jindo/JindoFileIO$BlobPresigner.class",
                            "org/apache/paimon/jindo/JindoLoader.class",
                            "META-INF/services/org.apache.paimon.fs.FileIOLoader")) {
                assertThat(entries)
                        .contains(rootEntry)
                        .doesNotContain(PLUGIN_DIRECTORY + rootEntry);
            }
            for (String resource :
                    Arrays.asList(
                            "versioninfo.properties", "common.properties", "oss.properties")) {
                assertThat(entries).contains(PLUGIN_DIRECTORY + resource).doesNotContain(resource);
            }
            for (String providedPackage :
                    Arrays.asList(
                            "com/aliyun/jindodata/",
                            "org/apache/hadoop/",
                            "org/slf4j/",
                            "org/apache/paimon/plugin/",
                            "org/apache/paimon/options/")) {
                assertThat(entries)
                        .noneMatch(
                                entry ->
                                        entry.startsWith(providedPackage)
                                                || entry.startsWith(
                                                        PLUGIN_DIRECTORY + providedPackage));
            }
        }
    }

    @Test
    @Tag("plugin-isolation")
    @EnabledIfSystemProperty(named = "jindo.plugin.jar", matches = ".+")
    public void testPluginWithMinimalHostClasspath() throws Exception {
        assertPluginClassLoading(false, false);
    }

    @Test
    @Tag("plugin-isolation")
    @EnabledIfSystemProperty(named = "jindo.plugin.jar", matches = ".+")
    public void testPluginBeforeHostDependencies() throws Exception {
        assertPluginClassLoading(true, true);
    }

    @Test
    @Tag("plugin-isolation")
    @EnabledIfSystemProperty(named = "jindo.plugin.jar", matches = ".+")
    public void testPluginAfterHostDependencies() throws Exception {
        assertPluginClassLoading(false, true);
    }

    private static File pluginJar() throws Exception {
        File jar = new File(System.getProperty("jindo.plugin.jar")).getCanonicalFile();
        assertThat(jar).isFile();
        return jar;
    }

    private static File classLocation(Class<?> type) throws Exception {
        return new File(type.getProtectionDomain().getCodeSource().getLocation().toURI())
                .getCanonicalFile();
    }

    private static Class<?> loadClassFrom(
            ClassLoader classLoader, String className, File expectedJar) throws Exception {
        Class<?> type = Class.forName(className, true, classLoader);
        assertThat(type.getClassLoader()).as("Loader of %s", className).isSameAs(classLoader);
        assertThat(classLocation(type)).as("Source of %s", className).isEqualTo(expectedJar);
        return type;
    }

    private void assertPluginClassLoading(boolean jarFirst, boolean conflictingDependencies)
            throws Exception {
        File jar = pluginJar();
        List<URL> urls = new ArrayList<>();
        urls.add(tempDir.toUri().toURL());
        List<String> hostClasses =
                new ArrayList<>(
                        Arrays.asList(
                                "org.apache.paimon.plugin.PluginLoader",
                                "org.apache.paimon.options.Options",
                                "org.apache.paimon.shade.guava30.com.google.common.collect.Iterators",
                                "com.aliyun.jindodata.Version",
                                "com.aliyun.jindodata.common.JindoHadoopSystem"));
        for (String artifact : Arrays.asList("hadoop-common", "slf4j-api", "commons-logging")) {
            urls.add(hostDependency(artifact).toURI().toURL());
        }
        if (conflictingDependencies) {
            hostClasses.addAll(
                    Arrays.asList(
                            "com.google.gson.Gson",
                            "org.apache.http.client.methods.HttpGet",
                            "org.apache.http.HttpVersion",
                            "org.apache.paimon.oss.OSSBlobPresigner",
                            "com.aliyun.oss.OSSClient"));
        }
        for (String className : hostClasses) {
            URL location = classLocation(testClass(className)).toURI().toURL();
            if (!urls.contains(location)) {
                urls.add(location);
            }
        }
        Files.write(
                tempDir.resolve("versioninfo.properties"),
                "version=host-version\n".getBytes(StandardCharsets.UTF_8));
        Files.write(
                tempDir.resolve("common.properties"),
                "ConnectionError=host-error\n".getBytes(StandardCharsets.UTF_8));
        urls.add(jarFirst ? 0 : urls.size(), jar.toURI().toURL());
        Thread thread = Thread.currentThread();
        ClassLoader contextClassLoader = thread.getContextClassLoader();
        try (URLClassLoader host =
                new URLClassLoader(
                        urls.toArray(new URL[0]), ClassLoader.getSystemClassLoader().getParent())) {
            thread.setContextClassLoader(host);
            assertThatThrownBy(() -> host.loadClass(PRESIGNER_IMPLEMENTATION))
                    .isInstanceOf(ClassNotFoundException.class);
            assertThat(host.getResource(PRESIGNER_IMPLEMENTATION.replace('.', '/') + ".class"))
                    .isNull();
            Class<?> optionsClass = host.loadClass(Options.class.getName());
            Map<String, String> settings = new HashMap<>();
            settings.put("fs.oss.endpoint", "oss.example.com");
            settings.put("fs.oss.region", "cn-hangzhou");
            settings.put("fs.oss.accessKeyId", "access-key");
            settings.put("fs.oss.accessKeySecret", "access-secret");
            settings.put("fs.oss.securityToken", "security-token");
            Object options = optionsClass.getConstructor(Map.class).newInstance(settings);
            Class<?> implementation = assertPublicEntryPoint(host, jar, options);
            try (URLClassLoader plugin = (URLClassLoader) implementation.getClassLoader()) {
                assertThat(implementation.getSimpleName()).isEqualTo("JindoBlobPresigner");
                assertThat(implementation.getEnclosingClass()).isNull();
                assertThat(implementation.getClassLoader()).isSameAs(plugin).isNotSameAs(host);
                assertThat(implementation.getInterfaces())
                        .contains(host.loadClass(JindoFileIO.BlobPresigner.class.getName()));
                assertThat(plugin.loadClass("org.apache.commons.logging.LogFactory"))
                        .isSameAs(host.loadClass("org.apache.commons.logging.LogFactory"));
                for (String className :
                        Arrays.asList(
                                "com.google.gson.Gson",
                                "org.apache.http.client.methods.HttpGet",
                                "org.apache.paimon.oss.OSSBlobPresigner",
                                "com.aliyun.oss.OSSClient")) {
                    Class<?> privateClass = Class.forName(className, true, plugin);
                    assertThat(privateClass.getClassLoader()).isSameAs(plugin);
                    assertThat(privateClass.getProtectionDomain().getCodeSource().getLocation())
                            .isEqualTo(host.getResource(PLUGIN_DIRECTORY));
                    if (conflictingDependencies) {
                        assertThat(
                                        loadClassFrom(
                                                host,
                                                className,
                                                classLocation(testClass(className))))
                                .isNotSameAs(privateClass);
                    } else {
                        assertThatThrownBy(() -> host.loadClass(className))
                                .isInstanceOf(ClassNotFoundException.class);
                        assertThat(host.getResource(className.replace('.', '/') + ".class"))
                                .isNull();
                    }
                }
                assertGson(plugin);
                if (conflictingDependencies) {
                    assertGson(host);
                }
                assertPluginResources(host, plugin);
                assertBlobClient(implementation, optionsClass, options, plugin);
                assertThat(thread.getContextClassLoader()).isSameAs(host);
            }
            Class<?> serviceClass = host.loadClass("org.apache.paimon.fs.FileIOLoader");
            boolean found = false;
            for (Object provider : ServiceLoader.load(serviceClass, host)) {
                if (provider.getClass().getName().equals("org.apache.paimon.jindo.JindoLoader")) {
                    loadClassFrom(host, provider.getClass().getName(), jar);
                    assertThat(serviceClass.getMethod("getScheme").invoke(provider))
                            .isEqualTo("oss");
                    found = true;
                    break;
                }
            }
            assertThat(found).as("Jindo FileIOLoader SPI from plugin artifact").isTrue();
        } finally {
            thread.setContextClassLoader(contextClassLoader);
        }
    }

    private static Class<?> testClass(String name) throws ClassNotFoundException {
        return Class.forName(name, false, JindoFileIOTest.class.getClassLoader());
    }

    private static File hostDependency(String artifact) throws IOException {
        String classPath =
                System.getProperty(
                        "surefire.test.class.path", System.getProperty("java.class.path"));
        for (String entry : classPath.split(File.pathSeparator)) {
            File dependency = new File(entry);
            if (dependency.isFile()
                    && dependency.getName().startsWith(artifact + "-")
                    && dependency.getName().endsWith(".jar")) {
                return dependency.getCanonicalFile();
            }
        }
        throw new AssertionError("Missing host dependency: " + artifact);
    }

    private static Class<?> assertPublicEntryPoint(ClassLoader host, File jar, Object options)
            throws Exception {
        Class<?> fileIOClass = loadClassFrom(host, "org.apache.paimon.jindo.JindoFileIO", jar);
        assertThat(fileIOClass.getDeclaredClasses())
                .contains(host.loadClass(JindoFileIO.BlobPresigner.class.getName()))
                .allSatisfy(nestedClass -> assertThat(nestedClass.getClassLoader()).isSameAs(host));
        Object fileIO = fileIOClass.getConstructor().newInstance();
        Field presignerField = fileIOClass.getDeclaredField("blobPresigner");
        presignerField.setAccessible(true);
        try {
            assertThat(presignerField.get(fileIO)).as("Presigner is lazy").isNull();
            Class<?> contextClass = host.loadClass(CatalogContext.class.getName());
            Object context =
                    contextClass.getMethod("create", options.getClass()).invoke(null, options);
            fileIOClass.getMethod("configure", contextClass).invoke(fileIO, context);
            assertThat(presignerField.get(fileIO))
                    .as("Configuration does not load the plugin")
                    .isNull();
            Class<?> pathClass = host.loadClass(Path.class.getName());
            Class<?> descriptorClass = host.loadClass(BlobDescriptor.class.getName());
            Object tableRoot =
                    pathClass.getConstructor(String.class).newInstance("oss://bucket/table");
            Object descriptor =
                    descriptorClass
                            .getConstructor(String.class, long.class, long.class)
                            .newInstance("oss://bucket/table/data/file", 10L, 20L);
            Method presign =
                    fileIOClass.getMethod(
                            "createBlobPresignedUrl", pathClass, descriptorClass, Duration.class);
            for (int attempt = 0; attempt < 2; attempt++) {
                assertThatThrownBy(
                                () -> presign.invoke(fileIO, tableRoot, descriptor, Duration.ZERO))
                        .isInstanceOf(InvocationTargetException.class)
                        .hasCauseInstanceOf(IOException.class)
                        .hasStackTraceContaining("positive whole seconds")
                        .hasStackTraceContaining(PRESIGNER_IMPLEMENTATION + ".create")
                        .hasStackTraceContaining("org.apache.paimon.oss.OSSBlobPresigner.create");
                assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(host);
            }
            Class<?> implementation = presignerField.get(fileIO).getClass();
            assertThat(implementation.getName()).isEqualTo(PRESIGNER_IMPLEMENTATION);
            return implementation;
        } finally {
            fileIOClass.getMethod("close").invoke(fileIO);
            assertThat(Thread.currentThread().getContextClassLoader()).isSameAs(host);
        }
    }

    private static void assertGson(ClassLoader loader) throws Exception {
        Class<?> gsonClass = loader.loadClass("com.google.gson.Gson");
        Object gson = gsonClass.getConstructor().newInstance();
        assertThat(
                        gsonClass
                                .getMethod("toJson", Object.class)
                                .invoke(gson, Collections.singletonMap("plugin", 1)))
                .isEqualTo("{\"plugin\":1}");
    }

    private static void assertBlobClient(
            Class<?> implementation, Class<?> optionsClass, Object options, ClassLoader plugin)
            throws Exception {
        Method createClient = implementation.getDeclaredMethod("createBlobClient", optionsClass);
        createClient.setAccessible(true);
        Class<?> clientClass = plugin.loadClass("com.aliyun.oss.OSSClient");
        assertThat(clientClass.getClassLoader()).isSameAs(plugin);
        Object client = createClient.invoke(null, options);
        try {
            assertThat(client.getClass()).isSameAs(clientClass);
            assertThat(clientClass.getMethod("getEndpoint").invoke(client))
                    .isEqualTo(URI.create("https://oss.example.com"));
            Object operation = clientClass.getMethod("getObjectOperation").invoke(client);
            assertThat(operation.getClass().getMethod("getRegion").invoke(operation))
                    .isEqualTo("cn-hangzhou");
            URL signedUrl =
                    (URL)
                            clientClass
                                    .getMethod(
                                            "generatePresignedUrl",
                                            String.class,
                                            String.class,
                                            Date.class)
                                    .invoke(
                                            client,
                                            "bucket",
                                            "object",
                                            new Date(System.currentTimeMillis() + 60_000));
            assertThat(signedUrl.getProtocol()).isEqualTo("https");
            assertThat(signedUrl.getHost()).isEqualTo("bucket.oss.example.com");
            assertThat(signedUrl.getPath()).isEqualTo("/object");
            assertThat(signedUrl.getQuery())
                    .contains(
                            "OSSAccessKeyId=access-key",
                            "Signature=",
                            "security-token=security-token");
        } finally {
            client.getClass().getMethod("shutdown").invoke(client);
        }
    }

    private static void assertPluginResources(ClassLoader host, ClassLoader plugin)
            throws Exception {
        for (String resource : Arrays.asList("versioninfo.properties", "common.properties")) {
            URL pluginResource = plugin.getResource(resource);
            assertThat(pluginResource).isNotNull();
            assertThat(pluginResource.toExternalForm())
                    .contains("!/" + PLUGIN_DIRECTORY + resource);
            assertThat(Collections.list(plugin.getResources(resource)))
                    .containsExactly(pluginResource);
        }
        assertThat(resourceProperties(host, "versioninfo.properties").getProperty("version"))
                .isEqualTo("host-version");
        assertThat(resourceProperties(host, "common.properties").getProperty("ConnectionError"))
                .isEqualTo("host-error");
        String version =
                resourceProperties(plugin, "versioninfo.properties").getProperty("version");
        assertThat(version).isNotBlank().isNotIn("host-version", "unknown-version");
        assertThat(
                        plugin.loadClass("com.aliyun.oss.common.utils.VersionInfoUtils")
                                .getMethod("getVersion")
                                .invoke(null))
                .isEqualTo(version);
        Class<?> managerClass = plugin.loadClass("com.aliyun.oss.common.utils.ResourceManager");
        Object manager =
                managerClass
                        .getMethod("getInstance", String.class, Locale.class)
                        .invoke(null, "common", Locale.ROOT);
        String message =
                resourceProperties(plugin, "common.properties").getProperty("ConnectionError");
        assertThat(message).isNotBlank().isNotEqualTo("host-error");
        assertThat(
                        managerClass
                                .getMethod("getString", String.class)
                                .invoke(manager, "ConnectionError"))
                .isEqualTo(message);
    }

    private static Properties resourceProperties(ClassLoader loader, String resource)
            throws IOException {
        Properties properties = new Properties();
        try (InputStream stream = loader.getResourceAsStream(resource)) {
            assertThat(stream).as(resource).isNotNull();
            properties.load(stream);
        }
        return properties;
    }

    private static class TestingJindoFileIO extends JindoFileIO {

        private final JindoHadoopSystem fs;
        private final org.apache.hadoop.fs.Path hadoopPath;

        private TestingJindoFileIO(JindoHadoopSystem fs, org.apache.hadoop.fs.Path hadoopPath) {
            this.fs = fs;
            this.hadoopPath = hadoopPath;
        }

        @Override
        protected org.apache.hadoop.fs.Path path(Path path) {
            return hadoopPath;
        }

        @Override
        protected Pair<JindoHadoopSystem, String> getFileSystemPair(
                org.apache.hadoop.fs.Path path, boolean enableCache) {
            return Pair.of(fs, "dls");
        }
    }

    private static String sha256Hex(byte[] bytes) throws Exception {
        StringBuilder result = new StringBuilder();
        for (byte value : MessageDigest.getInstance("SHA-256").digest(bytes)) {
            result.append(String.format("%02x", value & 0xff));
        }
        return result.toString();
    }
}
