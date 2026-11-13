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

package org.apache.paimon.spark.ossie

import org.apache.paimon.view.SemanticViewDefinition

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.networknt.schema.{SchemaRegistry, SpecificationVersion}
import org.yaml.snakeyaml.{LoaderOptions, Yaml}
import org.yaml.snakeyaml.constructor.SafeConstructor

import java.util.{Collections, IdentityHashMap, Locale}

import scala.collection.JavaConverters._

case class OssieField(name: String, sql: String)
case class OssieMetric(name: String, sql: String, modelScoped: Boolean)
case class OssieModel(
    dataset: String,
    source: String,
    fields: Seq[OssieField],
    metrics: Seq[OssieMetric])

/** Read-only execution profile of Ossie 0.3.0.dev0; the original document is never rewritten. */
object OssieModel {
  // The development version is mutable. Keep this schema and the execution profile together.
  val schemaCommit: String = "2692b1cc4708fdcf6a91e597ca928a9bc9d0da60"

  private val mapper = new ObjectMapper()

  private lazy val schema = {
    val stream = getClass.getResourceAsStream("ossie-schema.json")
    require(stream != null, "Missing bundled Ossie schema")
    try {
      SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_2020_12).getSchema(stream)
    } finally {
      stream.close()
    }
  }

  def parse(definition: SemanticViewDefinition): OssieModel = {
    require(definition.getFormat == "ossie-yaml", "Semantic query requires format ossie-yaml")
    val options = new LoaderOptions()
    options.setAllowDuplicateKeys(false)
    val document = new Yaml(new SafeConstructor(options)).load[AnyRef](definition.getContent)
    require(document != null, "Empty Ossie document")
    checkJson(document, Collections.newSetFromMap(new IdentityHashMap[AnyRef, java.lang.Boolean]()))
    val root = mapper.valueToTree[JsonNode](document)
    val errors = schema.validate(root).asScala
    require(errors.isEmpty, "Invalid Ossie 0.3.0.dev0 model: " + errors.take(5).mkString("; "))
    val datasets = elements(root, "datasets")
    require(datasets.size == 1, "Ossie queries currently require exactly one dataset")
    require(elements(root, "relationships").isEmpty, "Ossie relationships are not supported yet")
    val dataset = datasets.head
    val fields = elements(dataset, "fields").map(f => OssieField(text(f, "name"), expression(f)))
    require(fields.nonEmpty, "Ossie queries require declared dataset fields")
    val metrics = elements(dataset, "metrics").map(
      m => OssieMetric(text(m, "name"), expression(m), modelScoped = false)) ++
      elements(root, "metrics").map(
        m => OssieMetric(text(m, "name"), expression(m), modelScoped = true))
    require(metrics.nonEmpty, "Ossie queries require at least one metric")
    val names = (fields.map(_.name) ++ metrics.map(_.name)).map(normalize)
    require(names.distinct.size == names.size, "Ossie field and metric names must be unique")
    val fieldNames = fields.map(f => normalize(f.name)).toSet
    val keys = elements(dataset, "primary_key") ++
      elements(dataset, "unique_keys").flatMap(_.elements().asScala)
    require(
      keys.forall(k => fieldNames.contains(normalize(k.asText()))),
      "Ossie keys must reference declared dataset fields")
    OssieModel(text(dataset, "name"), text(dataset, "source"), fields, metrics)
  }

  def normalize(name: String): String = name.toLowerCase(Locale.ROOT)

  private def expression(node: JsonNode): String = {
    val expr = node.get("expression")
    if (expr.has("sql")) {
      text(expr, "sql")
    } else {
      val portable = elements(expr, "dialects").filter(d => text(d, "dialect") == "OSSIE_SQL_2026")
      require(portable.size == 1, "Ossie query requires an OSSIE_SQL_2026 expression")
      text(portable.head, "expression")
    }
  }

  private def text(node: JsonNode, name: String): String = node.get(name).asText()

  private def elements(node: JsonNode, name: String): Seq[JsonNode] =
    Option(node.get(name)).map(_.elements().asScala.toVector).getOrElse(Vector.empty)

  // Preserve YAML scalar types until validation: Jackson's YAML reader turns timestamps into
  // strings and map keys into strings, hiding values that Ossie forbids in extension data.
  private def checkJson(value: AnyRef, ancestors: java.util.Set[AnyRef]): Unit = value match {
    case null | _: String | _: java.lang.Boolean =>
    case n: java.lang.Double =>
      require(java.lang.Double.isFinite(n), "Ossie values must be finite JSON numbers")
    case n: java.lang.Float =>
      require(java.lang.Float.isFinite(n), "Ossie values must be finite JSON numbers")
    case _: java.lang.Number =>
    case map: java.util.Map[_, _] =>
      require(ancestors.add(map), "Ossie YAML cannot contain recursive aliases")
      try {
        map.asScala.foreach {
          case (key, item) =>
            require(key.isInstanceOf[String], "Ossie JSON object keys must be strings")
            checkJson(item.asInstanceOf[AnyRef], ancestors)
        }
      } finally { ancestors.remove(map) }
    case list: java.util.List[_] =>
      require(ancestors.add(list), "Ossie YAML cannot contain recursive aliases")
      try { list.asScala.foreach(item => checkJson(item.asInstanceOf[AnyRef], ancestors)) }
      finally { ancestors.remove(list) }
    case _ => throw new IllegalArgumentException("Ossie YAML values must be representable as JSON")
  }
}
