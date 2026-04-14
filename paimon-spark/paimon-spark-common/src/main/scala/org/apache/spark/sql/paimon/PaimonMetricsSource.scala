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

package org.apache.spark.sql.paimon

import com.codahale.metrics.jmx.JmxReporter
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.source.Source

/**
 * A Spark [[Source]] that holds a shared Codahale [[com.codahale.metrics.MetricRegistry]]. Paimon
 * metrics registered via [[org.apache.paimon.spark.metric.SparkMetricGroup]] are exposed through
 * this source.
 *
 * A [[JmxReporter]] is started on the registry so that metrics are registered as JMX MBeans
 * immediately when added. This is necessary because Spark's MetricsSystem takes a snapshot of the
 * registry at `registerSource` time and does not pick up metrics added later.
 *
 * This class must live under `org.apache.spark` because Spark's [[Source]] trait is
 * package-private.
 *
 * Use [[PaimonMetricsSource.getOrCreate()]] to obtain the singleton instance.
 */
class PaimonMetricsSource extends Source {

  override val sourceName: String = "paimon"

  override val metricRegistry: com.codahale.metrics.MetricRegistry =
    new com.codahale.metrics.MetricRegistry()

  private val jmxReporter: JmxReporter = JmxReporter
    .forRegistry(metricRegistry)
    .inDomain("paimon")
    .build()

  jmxReporter.start()
}

object PaimonMetricsSource {

  @volatile private var instance: PaimonMetricsSource = _

  def getOrCreate(): PaimonMetricsSource = {
    if (instance == null) {
      synchronized {
        if (instance == null) {
          val source = new PaimonMetricsSource()
          val env = SparkEnv.get
          if (env != null) {
            env.metricsSystem.registerSource(source)
          }
          instance = source
        }
      }
    }
    instance
  }
}
