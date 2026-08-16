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
 * No-op default for Spark versions without `SupportsPushDownVariantExtractions` (3.x / 4.0). The
 * 4.1 module overrides this trait at the same FQN to mix in the Spark interface. See
 * [[PaimonSupportsRuntimeFiltering]] for the same shadowing pattern.
 */
trait PaimonSupportsPushDownVariantExtractions {
  protected var acceptedVariantExtractions: Map[Seq[String], Seq[VariantExtractionInfo]] = Map.empty
}
