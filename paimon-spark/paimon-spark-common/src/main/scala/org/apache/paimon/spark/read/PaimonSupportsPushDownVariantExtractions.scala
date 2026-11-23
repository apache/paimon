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

package org.apache.paimon.spark.read

/**
 * No-op on every Spark version: Paimon accepts no variant extractions. On Spark 4.1 and 4.2, once a
 * scan accepts one, Spark builds it without column pruning and without its metadata columns (fixed
 * by SPARK-57499 in Spark 4.3). Spark 4.3 still cannot plan a partly accepted call, so a version
 * module that overrides this trait at the same FQN must accept all extractions or none. See
 * [[PaimonSupportsRuntimeFiltering]] for the same shadowing pattern.
 */
trait PaimonSupportsPushDownVariantExtractions {
  protected var acceptedVariantExtractions: Map[Seq[String], Seq[VariantExtractionInfo]] = Map.empty
}
