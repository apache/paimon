/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.store.hive;

import org.apache.flink.table.store.JobConfWrapper;
import org.apache.flink.table.store.mapred.TableStoreInputFormat;
import org.apache.flink.table.store.mapred.TableStoreOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/** {@link HiveStorageHandler} for table store. This is the entrance class of Hive API. */
public class TableStoreHiveStorageHandler implements HiveStorageHandler {

    private static final Logger LOG = LoggerFactory.getLogger(TableStoreHiveStorageHandler.class);

    private Configuration conf;

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return TableStoreInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return TableStoreOutputFormat.class;
    }

    @Override
    public Class<? extends AbstractSerDe> getSerDeClass() {
        return TableStoreSerDe.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return new TableStoreHiveMetaHook();
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return null;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> map) {
        Properties properties = tableDesc.getProperties();
        JobConfWrapper.update(properties, map);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> map) {}

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> map) {}

    @Override
    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
        JobConfWrapper wrapper = new JobConfWrapper(jobConf);
        wrapper.setFileStoreUser(UUID.randomUUID().toString());
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }
}
