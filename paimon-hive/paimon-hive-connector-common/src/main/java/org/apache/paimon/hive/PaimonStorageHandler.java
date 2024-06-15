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

package org.apache.paimon.hive;

import org.apache.paimon.hive.mapred.PaimonInputFormat;
import org.apache.paimon.hive.mapred.PaimonOutputCommitter;
import org.apache.paimon.hive.mapred.PaimonOutputFormat;
import org.apache.paimon.utils.JsonSerdeUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveStoragePredicateHandler;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

import java.util.Map;
import java.util.Properties;

/** {@link HiveStorageHandler} for paimon. This is the entrance class of Hive API. */
public class PaimonStorageHandler implements HiveStoragePredicateHandler, HiveStorageHandler {

    private static final String MAPRED_OUTPUT_COMMITTER = "mapred.output.committer.class";
    private static final String PAIMON_WRITE = "paimon.write";

    public static final String PAIMON_TABLE_FIELDS = "paimon.table.fields";

    private Configuration conf;

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return PaimonInputFormat.class;
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return PaimonOutputFormat.class;
    }

    @Override
    public Class<? extends AbstractSerDe> getSerDeClass() {
        return PaimonSerDe.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return new PaimonMetaHook(this.conf);
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider() throws HiveException {
        return null;
    }

    @Override
    public void configureInputJobProperties(TableDesc tableDesc, Map<String, String> map) {
        Properties properties = tableDesc.getProperties();
        String paimonLocation = LocationKeyExtractor.getPaimonLocation(conf, properties);
        map.put(LocationKeyExtractor.INTERNAL_LOCATION, paimonLocation);
        String dataFieldJsonStr = getDataFieldsJsonStr(properties);
        tableDesc.getProperties().put(PAIMON_TABLE_FIELDS, dataFieldJsonStr);
    }

    static String getDataFieldsJsonStr(Properties properties) {
        HiveSchema hiveSchema = HiveSchema.extract(null, properties);
        return JsonSerdeUtil.toJson(hiveSchema.fields());
    }

    public void configureInputJobCredentials(TableDesc tableDesc, Map<String, String> map) {}

    @Override
    public void configureOutputJobProperties(TableDesc tableDesc, Map<String, String> map) {
        Properties properties = tableDesc.getProperties();
        map.put(
                LocationKeyExtractor.INTERNAL_LOCATION,
                LocationKeyExtractor.getPaimonLocation(conf, properties));
        map.put(MAPRED_OUTPUT_COMMITTER, PaimonOutputCommitter.class.getName());
        map.put(PAIMON_WRITE, Boolean.TRUE.toString());
        properties.put(PAIMON_WRITE, Boolean.TRUE.toString());
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> map) {}

    @Override
    public void configureJobConf(TableDesc tableDesc, JobConf jobConf) {
        if (tableDesc != null
                && tableDesc.getProperties() != null
                && tableDesc.getProperties().get(PAIMON_WRITE) != null) {

            jobConf.set(MAPRED_OUTPUT_COMMITTER, PaimonOutputCommitter.class.getName());
        }
    }

    @Override
    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public DecomposedPredicate decomposePredicate(
            JobConf jobConf, Deserializer deserializer, ExprNodeDesc predicate) {
        DecomposedPredicate decomposed = new DecomposedPredicate();
        decomposed.residualPredicate = (ExprNodeGenericFuncDesc) predicate;
        decomposed.pushedPredicate = (ExprNodeGenericFuncDesc) predicate;
        return decomposed;
    }
}
