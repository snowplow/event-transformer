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

// java
import java.time.Instant
import java.util.UUID
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

// cats
import cats.data.Validated.{Invalid, Valid}
import cats.data.NonEmptyList
import cats.syntax.either._

// circe
import io.circe.{Decoder, Encoder, Json, JsonObject}
import io.circe.syntax._
import io.circe.parser._
import io.circe.generic.semiauto._
import io.circe.literal._

// Specs2
import org.specs2.mutable.Specification

// Iglu
import com.snowplowanalytics.iglu.core.{SchemaKey, SelfDescribingData}

/**
 * Tests Event case class
 */
class SnowplowEventSpec extends Specification {
  import EventSpec._

  "Contexts toShreddedJson" should {
    "return a map of JSON entities, keyed by column name" in {

      val sdd1 = SelfDescribingData[Json](SchemaKey.fromUri("iglu:myvendor1/myname1/jsonschema/1-2-3").toOption.get, json"""{"xyz": 42}""")
      val sdd2 =
        SelfDescribingData[Json](SchemaKey.fromUri("iglu:myvendor2/myname2/jsonschema/2-3-4").toOption.get, json"""{"abc": true}""")

      val input = SnowplowEvent.Contexts(List(sdd1, sdd2))

      val result = input.toShreddedJson

      val expected = Map(
        "contexts_myvendor1_myname1_1" -> json"""[{"_schema_version": "1-2-3", "xyz": 42}]""",
        "contexts_myvendor2_myname2_2" -> json"""[{"_schema_version": "2-3-4", "abc": true}]"""
      )

      result must beEqualTo(expected)

    }

    "return a map of JSON entities for all types of JSON value (object, array, string, number, boolean, null)" in {

      def sdd(version: Int, v: Json): SelfDescribingData[Json] =
        SelfDescribingData[Json](SchemaKey.fromUri(s"iglu:myvendor/myname/jsonschema/$version-0-0").toOption.get, v)

      val input = SnowplowEvent.Contexts(
        List(
          sdd(1, json"""{"xyz": 123}"""),
          sdd(2, json"""[1, 2, 3]"""),
          sdd(3, json""""foo""""),
          sdd(4, json"""42"""),
          sdd(5, json"""true"""),
          sdd(6, json"""null""")
        )
      )

      val result = input.toShreddedJson

      val expected = Map(
        "contexts_myvendor_myname_1" -> json"""[{"_schema_version": "1-0-0", "xyz": 123}]""",
        "contexts_myvendor_myname_2" -> json"""[[1, 2, 3]]""",
        "contexts_myvendor_myname_3" -> json"""["foo"]""",
        "contexts_myvendor_myname_4" -> json"""[42]""",
        "contexts_myvendor_myname_5" -> json"""[true]""",
        "contexts_myvendor_myname_6" -> json"""[null]"""
      )

      result must beEqualTo(expected)

    }
  }
}
