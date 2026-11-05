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

package org.apache.paimon.format.parquet;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.parquet.conf.ParquetConfiguration;
import org.apache.parquet.hadoop.BadConfigurationException;
import org.apache.parquet.hadoop.CodecFactory;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ConfigurationUtil;

import java.lang.reflect.Constructor;

/**
 * A {@link CodecFactory} that instantiates codecs without Hadoop's {@code ReflectionUtils}.
 *
 * <p>{@code ReflectionUtils.newInstance} caches the constructor of every class it creates in a
 * static map with strong keys. When Hadoop is loaded by a shared class loader, for example from
 * {@code /opt/flink/lib} of a Flink session cluster, while Paimon comes with the job jar, the keys
 * are Parquet's own codec classes (such as {@code ZstandardCodec}) loaded by the job class loader.
 * The map then keeps every finished job's class loader, and its Metaspace, reachable for the
 * lifetime of the process.
 */
public class ParquetCodecFactory extends CodecFactory {

    public ParquetCodecFactory(Configuration configuration, int pageSize) {
        super(configuration, pageSize);
    }

    public ParquetCodecFactory(ParquetConfiguration configuration, int pageSize) {
        super(configuration, pageSize);
    }

    @Override
    protected CompressionCodec getCodec(CompressionCodecName codecName) {
        String codecClassName = codecName.getHadoopCompressionCodecClassName();
        if (codecClassName == null) {
            return null;
        }
        String codecCacheKey = cacheKey(codecName);
        CompressionCodec codec = CODEC_BY_NAME.get(codecCacheKey);
        if (codec != null) {
            return codec;
        }

        Class<?> codecClass;
        try {
            try {
                codecClass = Class.forName(codecClassName);
            } catch (ClassNotFoundException e) {
                // Try to load the class using the job classloader
                codecClass = new Configuration(false).getClassLoader().loadClass(codecClassName);
            }
        } catch (ClassNotFoundException e) {
            throw new BadConfigurationException("Class " + codecClassName + " was not found", e);
        }

        // What ReflectionUtils.newInstance(codecClass, conf) does for a codec, without putting
        // codecClass into its static constructor cache.
        try {
            Constructor<?> constructor = codecClass.getDeclaredConstructor();
            constructor.setAccessible(true);
            codec = (CompressionCodec) constructor.newInstance();
        } catch (ReflectiveOperationException e) {
            throw new RuntimeException("Failed to instantiate codec " + codecClassName, e);
        }
        if (codec instanceof Configurable) {
            ((Configurable) codec).setConf(ConfigurationUtil.createHadoopConfiguration(conf));
        }
        CODEC_BY_NAME.put(codecCacheKey, codec);
        return codec;
    }

    /** Same key as the private {@code CodecFactory#cacheKey}. */
    private String cacheKey(CompressionCodecName codecName) {
        String level = null;
        switch (codecName) {
            case GZIP:
                level = conf.get("zlib.compress.level");
                break;
            case BROTLI:
                level = conf.get("compression.brotli.quality");
                break;
            case ZSTD:
                level = conf.get("parquet.compression.codec.zstd.level");
                break;
            default:
                // compression level is not supported; ignore it
        }
        String codecClass = codecName.getHadoopCompressionCodecClassName();
        return level == null ? codecClass : codecClass + ":" + level;
    }
}
