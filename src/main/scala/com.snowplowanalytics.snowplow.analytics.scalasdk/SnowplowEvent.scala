/*
 * Copyright (c) 2016-2020 Snowplow Analytics Ltd. All rights reserved.
 *
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License Version 2.0.
 * You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.
 */
package com.snowplowanalytics.snowplow.analytics.scalasdk

// circe
import io.circe.syntax._
import io.circe.{Decoder, DecodingFailure, Encoder, Json, JsonObject}
import io.circe.CursorOp.DownField

//cats
import cats.implicits._

// iglu
import com.snowplowanalytics.iglu.core.circe.CirceIgluCodecs._
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}

object SnowplowEvent {

  /**
   * A JSON representation of an atomic event's unstruct_event field.
   *
   * @param data the unstruct event as self-describing JSON, or None if the field is missing
   */
  case class UnstructEvent(data: Option[SelfDescribingData[Json]]) extends AnyVal {
    def toShreddedJson: Option[(String, Json)] =
      data.map {
        case SelfDescribingData(s, d) =>
          (transformSchema(Data.UnstructEvent, s.vendor, s.name, s.version.model), d)
      }
  }

  implicit final val unstructCirceEncoder: Encoder[UnstructEvent] =
    Encoder.instance { unstructEvent =>
      if (unstructEvent.data.isEmpty) Json.Null
      else
        JsonObject(
          ("schema", Common.UnstructEventUri.toSchemaUri.asJson),
          ("data", unstructEvent.data.asJson)
        ).asJson
    }

  implicit val unstructEventDecoder: Decoder[UnstructEvent] = Decoder.forProduct1("data")(UnstructEvent.apply).recover {
    case DecodingFailure(_, DownField("data") :: _) => UnstructEvent(None)
  }

  /**
   * A JSON representation of an atomic event's contexts or derived_contexts fields.
   *
   * @param data the context as self-describing JSON, or None if the field is missing
   */
  case class Contexts(data: List[SelfDescribingData[Json]]) extends AnyVal {
    def toShreddedJson: Map[String, Json] =
      data.groupBy(x => (x.schema.vendor, x.schema.name, x.schema.format, x.schema.version.model)).map {
        case ((vendor, name, _, model), contextsSdd) =>
          val transformedName = transformSchema(Data.Contexts(Data.CustomContexts), vendor, name, model)
          val transformedData = contextsSdd.map(addSchemaVersionToData).asJson
          (transformedName, transformedData)
      }
  }

  private def addSchemaVersionToData(contextSdd: SelfDescribingData[Json]): Json =
    if (contextSdd.data.isObject) {
      val version = Json.obj("_schema_version" -> contextSdd.schema.version.asString.asJson)
      contextSdd.data.deepMerge(version)
    } else contextSdd.data

  implicit final val contextsCirceEncoder: Encoder[Contexts] =
    Encoder.instance { contexts =>
      if (contexts.data.isEmpty) JsonObject.empty.asJson
      else
        JsonObject(
          ("schema", Common.ContextsUri.toSchemaUri.asJson),
          ("data", contexts.data.asJson)
        ).asJson
    }

  implicit val contextsDecoder: Decoder[Contexts] = Decoder.forProduct1("data")(Contexts.apply).recover {
    case DecodingFailure(_, DownField("data") :: _) => Contexts(List())
  }

  /**
   * @param shredProperty Type of self-describing entity
   * @param vendor        Iglu schema vendor
   * @param name          Iglu schema name
   * @param model         Iglu schema model
   * @return the schema, transformed into an Elasticsearch-compatible column name
   */
  def transformSchema(
    shredProperty: Data.ShredProperty,
    vendor: String,
    name: String,
    model: Int
  ): String = {
    // Convert dots & dashes in schema vendor to underscore
    val snakeCaseVendor = vendor.replaceAll("""[\.\-]""", "_").toLowerCase

    // Convert PascalCase in schema name to snake_case
    val snakeCaseName = name.replaceAll("""[\.\-]""", "_").replaceAll("([^A-Z_])([A-Z])", "$1_$2").toLowerCase

    s"${shredProperty.prefix}${snakeCaseVendor}_${snakeCaseName}_$model"
  }

  def transformSchema(shredProperty: Data.ShredProperty, schema: SchemaKey): String =
    transformSchema(shredProperty, schema.vendor, schema.name, schema.version.model)
}
