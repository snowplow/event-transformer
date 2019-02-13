/*
 * Copyright (c) 2016-2018 Snowplow Analytics Ltd. All rights reserved.
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
package com.snowplowanalytics.snowplow
package analytics.scalasdk
package json

// json4s
import org.json4s._
import org.json4s.JsonDSL._

// Specs2
import org.specs2.mutable.Specification

// This library
import Data._

/**
 * Tests Shredder
 */
class JsonShredderSpec extends Specification {

  "The fixSchema method" should {
    "convert a snake_case schema to an Elasticsearch field name" in {
      val actual = fixSchema(UnstructEvent, "iglu:com.snowplowanalytics.snowplow/change_form/jsonschema/1-0-0")
      actual must beEqualTo("unstruct_event_com_snowplowanalytics_snowplow_change_form_1")
    }

    "convert a PascalCase schema to an Elasticsearch field name" in {
      val actual = fixSchema(Contexts(DerivedContexts), "iglu:com.acme/PascalCaseContext/jsonschema/1-0-0")
      actual must beEqualTo("contexts_com_acme_pascal_case_context_1")
    }

    "convert a schema with consecutive capital letters to an Elasticsearch field name" in {
      val actual = fixSchema(Contexts(CustomContexts), "iglu:com.acme/ContextUK/jsonschema/1-0-0")
      actual must beEqualTo("contexts_com_acme_context_uk_1")
    }

    "convert a schema with hyphens Elasticsearch field name" in {
      val actual = fixSchema(Contexts(CustomContexts), "iglu:com.acme/lisp-case/jsonschema/1-0-0")
      actual must beEqualTo("contexts_com_acme_lisp_case_1")
    }
  }

  "The parseUnstruct method" should {
    "fix up an unstructured event JSON" in {
      val actual = JsonShredder.parseUnstruct("""{
        "schema": "any",
        "data": {
          "schema": "iglu:com.snowplowanalytics.snowplow/social_interaction/jsonschema/1-0-0",
          "data": {
            "action": "like",
            "network": "fb"
          }
        }
      }""")
      val expected = UnstructEventOutput(
        "iglu:com.snowplowanalytics.snowplow/social_interaction/jsonschema/1-0-0",
        JObject(("action", "like"), ("network", "fb"))
      )

      actual must beRight(expected)
    }

    "fail a malformed unstructured event JSON" in {
      val actual = JsonShredder.parseUnstruct("""{
        "schema": "any",
        "data": {}
      }""")

      val expected = List(
        "Unstructured event JSON did not contain a stringly typed schema field",
        "Could not extract inner data field from unstructured event")

      actual must beLeft.like {
        case errors => errors must beEqualTo(expected)
      }
    }
  }

  "The parseContexts method" should {
    "fix up a custom contexts JSON" in {
      val actual = JsonShredder.parseContexts(CustomContexts)("""{
        "schema": "any",
        "data": [
          {
            "schema": "iglu:com.acme/duplicated/jsonschema/20-0-5",
            "data": {
              "value": 1
            }
          },
          {
            "schema": "iglu:com.acme/duplicated/jsonschema/20-0-5",
            "data": {
              "value": 2
            }
          },
          {
            "schema": "iglu:com.acme/unduplicated/jsonschema/1-0-0",
            "data": {
              "type": "test"
            }
          }
        ]
      }""")
      val expected: Map[String, List[JValue]] = Map(
        "iglu:com.acme/duplicated/jsonschema/20-0-5" -> List(("value", 2), ("value", 1)),
        "iglu:com.acme/unduplicated/jsonschema/1-0-0" -> List(("type", "test"))
      )

      actual must beRight(ContextsOutput(CustomContexts, expected))
    }

    "fail a malformed custom contexts JSON" in {
      val actual = JsonShredder.parseContexts(DerivedContexts)("""{
        "schema": "any",
        "data": [
          {
            "schema": "failing",
            "data": {
              "value": 1
            }
          },
          {
            "data": {
              "value": 2
            }
          },
          {
            "schema": "iglu:com.acme/unduplicated/jsonschema/1-0-0"
          }
        ]
      }""")

      val expected = List(
        "Could not extract inner data field from custom context",
        "Context JSON did not contain a stringly typed schema field",
        "Schema [failing] does not conform to Iglu Schema URI regular expression")

      actual must beLeft.like {
        case errors => errors must beEqualTo(expected)
      }
    }

    "allow empty custom context" in {
      val actual = JsonShredder.parseContexts(CustomContexts)("""{
        "schema": "any",
        "data": []
      }""")

      actual must beRight
    }
  }
}
