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

package org.apache.paimon.spark.metric

import org.apache.paimon.metrics.{Counter, Gauge, Histogram, MetricGroupImpl}

import com.codahale.metrics.{Gauge => CodahaleGauge, MetricRegistry => CodahaleMetricRegistry}

import java.util.{Map => JMap}

import scala.collection.JavaConverters._

/** Keeps Paimon's Spark UI metrics while publishing their current values to JMX. */
class SparkMetricGroup(
    groupName: String,
    variables: JMap[String, String],
    registry: CodahaleMetricRegistry)
  extends MetricGroupImpl(groupName, variables) {

  override def counter(name: String): Counter = {
    val metric = super.counter(name)
    register(name, () => java.lang.Long.valueOf(metric.getCount))
    metric
  }

  override def gauge[T](name: String, gauge: Gauge[T]): Gauge[T] = {
    val metric = super.gauge(name, gauge)
    if (metric != null) {
      register(name, () => metric.getValue.asInstanceOf[AnyRef])
    }
    metric
  }

  override def histogram(name: String, windowSize: Int): Histogram = {
    val metric = super.histogram(name, windowSize)
    register(name, () => java.lang.Double.valueOf(metric.getStatistics.getMean))
    metric
  }

  private def register(name: String, value: () => AnyRef): Unit = {
    val path = (Seq(groupName) ++ variables.asScala.toSeq.sortBy(_._1).flatMap {
      case (key, variable) => Seq(key, variable)
    } :+ name).mkString(".")
    val jmxGauge = new CodahaleGauge[AnyRef] {
      override def getValue: AnyRef = value()
    }

    // Keep the latest value available to periodic scrapers; repeated operations on the same
    // table replace their metrics rather than increasing the registry's cardinality.
    registry.synchronized {
      registry.remove(path)
      registry.register(path, jmxGauge)
    }
  }
}
