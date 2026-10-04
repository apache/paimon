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

package org.apache.spark.metrics.source

import com.codahale.metrics.{MetricRegistry => CodahaleMetricRegistry}
import com.codahale.metrics.jmx.JmxReporter
import org.apache.spark.SparkEnv
import org.apache.spark.metrics.MetricsSystem

/** Spark source for Paimon metrics. The reporter observes metrics added after source creation. */
class PaimonMetricsSource extends Source {

  override val sourceName: String = "paimon"

  override def metricRegistry: CodahaleMetricRegistry = PaimonMetricsSource.sharedRegistry
}

object PaimonMetricsSource {

  private val sharedRegistry = new CodahaleMetricRegistry()

  private val reporter = JmxReporter.forRegistry(sharedRegistry).inDomain("paimon").build()
  reporter.start()

  // Spark snapshots a source's registry when it is registered, so the reporter above also
  // observes metrics created later by scans and commits.
  private val source = new PaimonMetricsSource

  @volatile private var registeredSystem: MetricsSystem = _

  def metricRegistry: CodahaleMetricRegistry = {
    Option(SparkEnv.get).foreach {
      env =>
        val system = env.metricsSystem
        if (registeredSystem ne system) {
          synchronized {
            if (registeredSystem ne system) {
              if (system.getSourcesByName(source.sourceName).isEmpty) {
                system.registerSource(source)
              }
              registeredSystem = system
            }
          }
        }
    }
    sharedRegistry
  }
}
