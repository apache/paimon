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
import org.apache.paimon.hive.mapred.PaimonRecordReader;

import org.junit.Before;

/** IT cases for {@link PaimonRecordReader} and {@link PaimonInputFormat} with Hive On Tez. */
public class HiveOnTezReadITCase extends HiveReadITCaseBase {

    @Before
    public void before() throws Exception {
        super.before();
        setHiveExecutionEngine();
    }

    @Override
    protected void setHiveExecutionEngine() {
        hiveShell.execute("SET hive.execution.engine=tez");
        hiveShell.execute("SET tez.local.mode=true");
        hiveShell.execute("SET hive.jar.directory=" + folder.getRoot().getAbsolutePath());
        hiveShell.execute("SET tez.staging-dir=" + folder.getRoot().getAbsolutePath());
        // JVM will crash if we do not set this and include paimon-flink-common as dependency
        // not sure why
        // in real use case there won't be any Flink dependency in Hive's classpath, so it's OK
        hiveShell.execute("SET hive.tez.exec.inplace.progress=false");
    }
}
