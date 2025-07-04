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

package org.apache.paimon.format.lance;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.local.LocalFileIO;
import org.apache.paimon.jindo.JindoFileIO;
import org.apache.paimon.options.Options;
import org.apache.paimon.oss.OSSFileIO;
import org.apache.paimon.utils.Pair;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

/** Utils for lance all. */
public class LanceUtils {

    private static final Class<?> ossFileIOKlass;
    private static final Class<?> jindoFileIOKlass;

    static {
        Class<?> klass;
        try {
            klass = Class.forName("org.apache.paimon.oss.OSSFileIO");
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            klass = null;
        }
        ossFileIOKlass = klass;

        try {
            klass = Class.forName("org.apache.paimon.jindo.JindoFileIO");
        } catch (ClassNotFoundException | NoClassDefFoundError e) {
            klass = null;
        }
        jindoFileIOKlass = klass;
    }

    public static Pair<Path, Map<String, String>> toLanceSpecified(FileIO fileIO, Path path) {
        boolean ossPath = false;
        Path converted;
        Map<String, String> storageOptions = new HashMap<>();
        Options originOptions;
        if (ossFileIOKlass != null && ossFileIOKlass.isInstance(fileIO)) {
            ossPath = true;
            originOptions = ((OSSFileIO) fileIO).hadoopOptions();
        } else if (jindoFileIOKlass != null && jindoFileIOKlass.isInstance(fileIO)) {
            ossPath = true;
            originOptions = ((JindoFileIO) fileIO).hadoopOptions();
        } else {
            originOptions = new Options();
        }

        if (ossPath) {
            URI uri = path.toUri();
            storageOptions.put(
                    "endpoint",
                    "https://" + uri.getHost() + "." + originOptions.get("fs.oss.endpoint"));
            storageOptions.put("access_key_id", originOptions.get("fs.oss.accessKeyId"));
            storageOptions.put("secret_access_key", originOptions.get("fs.oss.accessKeySecret"));
            storageOptions.put("virtual_hosted_style_request", "true");
            converted = new Path(uri.toString().replace("oss://", "s3://"));
        } else if (fileIO instanceof LocalFileIO) {
            converted = path;
        } else {
            // TODO: support other FileIO types
            throw new UnsupportedOperationException(
                    "Unsupported FileIO type: " + fileIO.getClass().getName());
        }

        return Pair.of(converted, storageOptions);
    }
}
