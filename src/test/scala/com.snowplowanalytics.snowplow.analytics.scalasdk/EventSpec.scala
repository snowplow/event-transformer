/*
 * Copyright (c) 2016-2019 Snowplow Analytics Ltd. All rights reserved.
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

// cats
import cats.data.Validated.{Invalid, Valid}
import cats.data.NonEmptyList

// circe
import io.circe.{Json, JsonObject}
import io.circe.syntax._
import io.circe.parser._

// Specs2
import org.specs2.mutable.Specification

// Iglu
import com.snowplowanalytics.iglu.core.{SchemaKey, SchemaVer, SelfDescribingData}

// This library
import com.snowplowanalytics.snowplow.analytics.scalasdk.SnowplowEvent.{Contexts, UnstructEvent}
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.EventTransformer
import com.snowplowanalytics.snowplow.analytics.scalasdk.json.Data

/**
  * Tests Event case class
  */
class EventSpec extends Specification {

  val unstructJson =
    """{
    "schema": "iglu:com.snowplowanalytics.snowplow/unstruct_event/jsonschema/1-0-0",
    "data": {
      "schema": "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1",
      "data": {
        "targetUrl": "http://www.example.com",
        "elementClasses": ["foreground"],
        "elementId": "exampleLink"
      }
    }
  }"""

  val contextsJson =
    """{
    "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
    "data": [
      {
        "schema": "iglu:org.schema/WebPage/jsonschema/1-0-0",
        "data": {
          "genre": "blog",
          "inLanguage": "en-US",
          "datePublished": "2014-11-06T00:00:00Z",
          "author": "Fred Blundun",
          "breadcrumb": [
            "blog",
            "releases"
          ],
          "keywords": [
            "snowplow",
            "javascript",
            "tracker",
            "event"
          ]
        }
      },
      {
        "schema": "iglu:org.w3/PerformanceTiming/jsonschema/1-0-0",
        "data": {
          "navigationStart": 1415358089861,
          "unloadEventStart": 1415358090270,
          "unloadEventEnd": 1415358090287,
          "redirectStart": 0,
          "redirectEnd": 0,
          "fetchStart": 1415358089870,
          "domainLookupStart": 1415358090102,
          "domainLookupEnd": 1415358090102,
          "connectStart": 1415358090103,
          "connectEnd": 1415358090183,
          "requestStart": 1415358090183,
          "responseStart": 1415358090265,
          "responseEnd": 1415358090265,
          "domLoading": 1415358090270,
          "domInteractive": 1415358090886,
          "domContentLoadedEventStart": 1415358090968,
          "domContentLoadedEventEnd": 1415358091309,
          "domComplete": 0,
          "loadEventStart": 0,
          "loadEventEnd": 0
        }
      }
    ]
  }"""

  val contextsWithDuplicate = """{
    "schema": "iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0",
    "data": [
      {
        "schema": "iglu:org.schema/WebPage/jsonschema/1-0-0",
        "data": {
          "genre": "blog",
          "inLanguage": "en-US",
          "datePublished": "2014-11-06T00:00:00Z",
          "author": "Fred Blundun",
          "breadcrumb": [
            "blog",
            "releases"
          ],
          "keywords": [
            "snowplow",
            "javascript",
            "tracker",
            "event"
          ]
        }
      },
      {
        "schema": "iglu:org.acme/context_one/jsonschema/1-0-0",
        "data": {
          "item": 1
        }
      },
      {
        "schema": "iglu:org.acme/context_one/jsonschema/1-0-1",
        "data": {
          "item": 2
        }
      }
    ]
  }"""

  val derivedContextsJson =
    """{
    "schema": "iglu:com.snowplowanalytics.snowplow\/contexts\/jsonschema\/1-0-1",
    "data": [
      {
        "schema": "iglu:com.snowplowanalytics.snowplow\/ua_parser_context\/jsonschema\/1-0-0",
        "data": {
          "useragentFamily": "IE",
          "useragentMajor": "7",
          "useragentMinor": "0",
          "useragentPatch": null,
          "useragentVersion": "IE 7.0",
          "osFamily": "Windows XP",
          "osMajor": null,
          "osMinor": null,
          "osPatch": null,
          "osPatchMinor": null,
          "osVersion": "Windows XP",
          "deviceFamily": "Other"
        }
      }
    ]
  }"""

  "The Event parser" should {
    "successfully convert a tab-separated pageview event string to an Event instance and JSON" in {

      val input = List(
        "app_id" -> "angry-birds",
        "platform" -> "web",
        "etl_tstamp" -> "2017-01-26 00:01:25.292",
        "collector_tstamp" -> "2013-11-26 00:02:05",
        "dvce_created_tstamp" -> "2013-11-26 00:03:57.885",
        "event" -> "page_view",
        "event_id" -> "c6ef3124-b53a-4b13-a233-0088f79dcbcb",
        "txn_id" -> "41828",
        "name_tracker" -> "cloudfront-1",
        "v_tracker" -> "js-2.1.0",
        "v_collector" -> "clj-tomcat-0.1.0",
        "v_etl" -> "serde-0.5.2",
        "user_id" -> "jon.doe@email.com",
        "user_ipaddress" -> "92.231.54.234",
        "user_fingerprint" -> "2161814971",
        "domain_userid" -> "bc2e92ec6c204a14",
        "domain_sessionidx" -> "3",
        "network_userid" -> "ecdff4d0-9175-40ac-a8bb-325c49733607",
        "geo_country" -> "US",
        "geo_region" -> "TX",
        "geo_city" -> "New York",
        "geo_zipcode" -> "94109",
        "geo_latitude" -> "37.443604",
        "geo_longitude" -> "-122.4124",
        "geo_region_name" -> "Florida",
        "ip_isp" -> "FDN Communications",
        "ip_organization" -> "Bouygues Telecom",
        "ip_domain" -> "nuvox.net",
        "ip_netspeed" -> "Cable/DSL",
        "page_url" -> "http://www.snowplowanalytics.com",
        "page_title" -> "On Analytics",
        "page_referrer" -> "",
        "page_urlscheme" -> "http",
        "page_urlhost" -> "www.snowplowanalytics.com",
        "page_urlport" -> "80",
        "page_urlpath" -> "/product/index.html",
        "page_urlquery" -> "id=GTM-DLRG",
        "page_urlfragment" -> "4-conclusion",
        "refr_urlscheme" -> "",
        "refr_urlhost" -> "",
        "refr_urlport" -> "",
        "refr_urlpath" -> "",
        "refr_urlquery" -> "",
        "refr_urlfragment" -> "",
        "refr_medium" -> "",
        "refr_source" -> "",
        "refr_term" -> "",
        "mkt_medium" -> "",
        "mkt_source" -> "",
        "mkt_term" -> "",
        "mkt_content" -> "",
        "mkt_campaign" -> "",
        "contexts" -> contextsJson,
        "se_category" -> "",
        "se_action" -> "",
        "se_label" -> "",
        "se_property" -> "",
        "se_value" -> "",
        "unstruct_event" -> unstructJson,
        "tr_orderid" -> "",
        "tr_affiliation" -> "",
        "tr_total" -> "",
        "tr_tax" -> "",
        "tr_shipping" -> "",
        "tr_city" -> "",
        "tr_state" -> "",
        "tr_country" -> "",
        "ti_orderid" -> "",
        "ti_sku" -> "",
        "ti_name" -> "",
        "ti_category" -> "",
        "ti_price" -> "",
        "ti_quantity" -> "",
        "pp_xoffset_min" -> "",
        "pp_xoffset_max" -> "",
        "pp_yoffset_min" -> "",
        "pp_yoffset_max" -> "",
        "useragent" -> "",
        "br_name" -> "",
        "br_family" -> "",
        "br_version" -> "",
        "br_type" -> "",
        "br_renderengine" -> "",
        "br_lang" -> "",
        "br_features_pdf" -> "1",
        "br_features_flash" -> "0",
        "br_features_java" -> "",
        "br_features_director" -> "",
        "br_features_quicktime" -> "",
        "br_features_realplayer" -> "",
        "br_features_windowsmedia" -> "",
        "br_features_gears" -> "",
        "br_features_silverlight" -> "",
        "br_cookies" -> "",
        "br_colordepth" -> "",
        "br_viewwidth" -> "",
        "br_viewheight" -> "",
        "os_name" -> "",
        "os_family" -> "",
        "os_manufacturer" -> "",
        "os_timezone" -> "",
        "dvce_type" -> "",
        "dvce_ismobile" -> "",
        "dvce_screenwidth" -> "",
        "dvce_screenheight" -> "",
        "doc_charset" -> "",
        "doc_width" -> "",
        "doc_height" -> "",
        "tr_currency" -> "",
        "tr_total_base" -> "",
        "tr_tax_base" -> "",
        "tr_shipping_base" -> "",
        "ti_currency" -> "",
        "ti_price_base" -> "",
        "base_currency" -> "",
        "geo_timezone" -> "",
        "mkt_clickid" -> "",
        "mkt_network" -> "",
        "etl_tags" -> "",
        "dvce_sent_tstamp" -> "",
        "refr_domain_userid" -> "",
        "refr_device_tstamp" -> "",
        "derived_contexts" -> derivedContextsJson,
        "domain_sessionid" -> "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1",
        "derived_tstamp" -> "2013-11-26 00:03:57.886",
        "event_vendor" -> "com.snowplowanalytics.snowplow",
        "event_name" -> "link_click",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event_fingerprint" -> "e3dbfa9cca0412c3d4052863cefb547f",
        "true_tstamp" -> "2013-11-26 00:03:57.886"
      )

      val expected = Event(
        app_id = Some("angry-birds"),
        platform = Some("web"),
        etl_tstamp = Some(Instant.parse("2017-01-26T00:01:25.292Z")),
        collector_tstamp = Instant.parse("2013-11-26T00:02:05Z"),
        dvce_created_tstamp = Some(Instant.parse("2013-11-26T00:03:57.885Z")),
        event = Some("page_view"),
        event_id = UUID.fromString("c6ef3124-b53a-4b13-a233-0088f79dcbcb"),
        txn_id = Some(41828),
        name_tracker = Some("cloudfront-1"),
        v_tracker = Some("js-2.1.0"),
        v_collector = "clj-tomcat-0.1.0",
        v_etl = "serde-0.5.2",
        user_id = Some("jon.doe@email.com"),
        user_ipaddress = Some("92.231.54.234"),
        user_fingerprint = Some("2161814971"),
        domain_userid = Some("bc2e92ec6c204a14"),
        domain_sessionidx = Some(3),
        network_userid = Some("ecdff4d0-9175-40ac-a8bb-325c49733607"),
        geo_country = Some("US"),
        geo_region = Some("TX"),
        geo_city = Some("New York"),
        geo_zipcode = Some("94109"),
        geo_latitude = Some(37.443604),
        geo_longitude = Some(-122.4124),
        geo_region_name = Some("Florida"),
        ip_isp = Some("FDN Communications"),
        ip_organization = Some("Bouygues Telecom"),
        ip_domain = Some("nuvox.net"),
        ip_netspeed = Some("Cable/DSL"),
        page_url = Some("http://www.snowplowanalytics.com"),
        page_title = Some("On Analytics"),
        page_referrer = None,
        page_urlscheme = Some("http"),
        page_urlhost = Some("www.snowplowanalytics.com"),
        page_urlport = Some(80),
        page_urlpath = Some("/product/index.html"),
        page_urlquery = Some("id=GTM-DLRG"),
        page_urlfragment = Some("4-conclusion"),
        refr_urlscheme = None,
        refr_urlhost = None,
        refr_urlport = None,
        refr_urlpath = None,
        refr_urlquery = None,
        refr_urlfragment = None,
        refr_medium = None,
        refr_source = None,
        refr_term = None,
        mkt_medium = None,
        mkt_source = None,
        mkt_term = None,
        mkt_content = None,
        mkt_campaign = None,
        contexts = Contexts(
          List(
            SelfDescribingData(
              SchemaKey(
                "org.schema",
                "WebPage",
                "jsonschema",
                SchemaVer.Full(1, 0, 0)
              ),
              JsonObject(
                ("genre", "blog".asJson),
                ("inLanguage", "en-US".asJson),
                ("datePublished", "2014-11-06T00:00:00Z".asJson),
                ("author", "Fred Blundun".asJson),
                ("breadcrumb", List("blog", "releases").asJson),
                ("keywords", List("snowplow", "javascript", "tracker", "event").asJson)
              ).asJson
            ),
            SelfDescribingData(
              SchemaKey(
                "org.w3",
                "PerformanceTiming",
                "jsonschema",
                SchemaVer.Full(1, 0, 0)
              ),
              JsonObject(
                ("navigationStart", 1415358089861L.asJson),
                ("unloadEventStart", 1415358090270L.asJson),
                ("unloadEventEnd", 1415358090287L.asJson),
                ("redirectStart", 0.asJson),
                ("redirectEnd", 0.asJson),
                ("fetchStart", 1415358089870L.asJson),
                ("domainLookupStart", 1415358090102L.asJson),
                ("domainLookupEnd", 1415358090102L.asJson),
                ("connectStart", 1415358090103L.asJson),
                ("connectEnd", 1415358090183L.asJson),
                ("requestStart", 1415358090183L.asJson),
                ("responseStart", 1415358090265L.asJson),
                ("responseEnd", 1415358090265L.asJson),
                ("domLoading", 1415358090270L.asJson),
                ("domInteractive", 1415358090886L.asJson),
                ("domContentLoadedEventStart", 1415358090968L.asJson),
                ("domContentLoadedEventEnd", 1415358091309L.asJson),
                ("domComplete", 0.asJson),
                ("loadEventStart", 0.asJson),
                ("loadEventEnd", 0.asJson)
              ).asJson
            )
          )
        ),
        se_category = None,
        se_action = None,
        se_label = None,
        se_property = None,
        se_value = None,
        unstruct_event = UnstructEvent(
          Some(
            SelfDescribingData(
              SchemaKey(
                "com.snowplowanalytics.snowplow",
                "link_click",
                "jsonschema",
                SchemaVer.Full(1, 0, 1)
              ),
              JsonObject(
                ("targetUrl", "http://www.example.com".asJson),
                ("elementClasses", List("foreground").asJson),
                ("elementId", "exampleLink".asJson)
              ).asJson
            )
          )
        ),
        tr_orderid = None,
        tr_affiliation = None,
        tr_total = None,
        tr_tax = None,
        tr_shipping = None,
        tr_city = None,
        tr_state = None,
        tr_country = None,
        ti_orderid = None,
        ti_sku = None,
        ti_name = None,
        ti_category = None,
        ti_price = None,
        ti_quantity = None,
        pp_xoffset_min = None,
        pp_xoffset_max = None,
        pp_yoffset_min = None,
        pp_yoffset_max = None,
        useragent = None,
        br_name = None,
        br_family = None,
        br_version = None,
        br_type = None,
        br_renderengine = None,
        br_lang = None,
        br_features_pdf = Some(true),
        br_features_flash = Some(false),
        br_features_java = None,
        br_features_director = None,
        br_features_quicktime = None,
        br_features_realplayer = None,
        br_features_windowsmedia = None,
        br_features_gears = None,
        br_features_silverlight = None,
        br_cookies = None,
        br_colordepth = None,
        br_viewwidth = None,
        br_viewheight = None,
        os_name = None,
        os_family = None,
        os_manufacturer = None,
        os_timezone = None,
        dvce_type = None,
        dvce_ismobile = None,
        dvce_screenwidth = None,
        dvce_screenheight = None,
        doc_charset = None,
        doc_width = None,
        doc_height = None,
        tr_currency = None,
        tr_total_base = None,
        tr_tax_base = None,
        tr_shipping_base = None,
        ti_currency = None,
        ti_price_base = None,
        base_currency = None,
        geo_timezone = None,
        mkt_clickid = None,
        mkt_network = None,
        etl_tags = None,
        dvce_sent_tstamp = None,
        refr_domain_userid = None,
        refr_device_tstamp = None,
        derived_contexts = Contexts(
          List(
            SelfDescribingData(
              SchemaKey(
                "com.snowplowanalytics.snowplow",
                "ua_parser_context",
                "jsonschema",
                SchemaVer.Full(1, 0, 0)
              ),
              JsonObject(
                ("useragentFamily", "IE".asJson),
                ("useragentMajor", "7".asJson),
                ("useragentMinor", "0".asJson),
                ("useragentPatch", Json.Null),
                ("useragentVersion", "IE 7.0".asJson),
                ("osFamily", "Windows XP".asJson),
                ("osMajor", Json.Null),
                ("osMinor", Json.Null),
                ("osPatch", Json.Null),
                ("osPatchMinor", Json.Null),
                ("osVersion", "Windows XP".asJson),
                ("deviceFamily", "Other".asJson)
              ).asJson
            )
          )
        ),
        domain_sessionid = Some("2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1"),
        derived_tstamp = Some(Instant.parse("2013-11-26T00:03:57.886Z")),
        event_vendor = Some("com.snowplowanalytics.snowplow"),
        event_name = Some("link_click"),
        event_format = Some("jsonschema"),
        event_version = Some("1-0-0"),
        event_fingerprint = Some("e3dbfa9cca0412c3d4052863cefb547f"),
        true_tstamp = Some(Instant.parse("2013-11-26T00:03:57.886Z"))
      )

      val eventValues = input.unzip._2.mkString("\t")
      val event = Event.parse(eventValues)

      // Case class must be processed as expected
      event mustEqual Valid(expected)

      val eventJson = event.getOrElse(throw new RuntimeException("Failed to parse event")).toJson(true)

      val legacyJson = parse(EventTransformer
        .transform(eventValues)
        .right
        .toOption
        .getOrElse(throw new RuntimeException("Event failed transformation")))
          .right
          .toOption
          .getOrElse(throw new RuntimeException("Event failed transformation"))

      // JSON output must be equal to output from the old transformer. (NB: field ordering in new JSON will be randomized)
      eventJson mustEqual legacyJson
    }

    "successfully convert a tab-separated pageview event string to an Event instance and JSON, omitting unstruct_event and contexts nullary fields" in {

      val input = List(
        "app_id" -> "angry-birds",
        "platform" -> "web",
        "etl_tstamp" -> "2017-01-26 00:01:25.292",
        "collector_tstamp" -> "2013-11-26 00:02:05",
        "dvce_created_tstamp" -> "2013-11-26 00:03:57.885",
        "event" -> "page_view",
        "event_id" -> "c6ef3124-b53a-4b13-a233-0088f79dcbcb",
        "txn_id" -> "41828",
        "name_tracker" -> "cloudfront-1",
        "v_tracker" -> "js-2.1.0",
        "v_collector" -> "clj-tomcat-0.1.0",
        "v_etl" -> "serde-0.5.2",
        "user_id" -> "jon.doe@email.com",
        "user_ipaddress" -> "92.231.54.234",
        "user_fingerprint" -> "2161814971",
        "domain_userid" -> "bc2e92ec6c204a14",
        "domain_sessionidx" -> "3",
        "network_userid" -> "ecdff4d0-9175-40ac-a8bb-325c49733607",
        "geo_country" -> "US",
        "geo_region" -> "TX",
        "geo_city" -> "New York",
        "geo_zipcode" -> "94109",
        "geo_latitude" -> "37.443604",
        "geo_longitude" -> "-122.4124",
        "geo_region_name" -> "Florida",
        "ip_isp" -> "FDN Communications",
        "ip_organization" -> "Bouygues Telecom",
        "ip_domain" -> "nuvox.net",
        "ip_netspeed" -> "Cable/DSL",
        "page_url" -> "http://www.snowplowanalytics.com",
        "page_title" -> "On Analytics",
        "page_referrer" -> "",
        "page_urlscheme" -> "http",
        "page_urlhost" -> "www.snowplowanalytics.com",
        "page_urlport" -> "80",
        "page_urlpath" -> "/product/index.html",
        "page_urlquery" -> "id=GTM-DLRG",
        "page_urlfragment" -> "4-conclusion",
        "refr_urlscheme" -> "",
        "refr_urlhost" -> "",
        "refr_urlport" -> "",
        "refr_urlpath" -> "",
        "refr_urlquery" -> "",
        "refr_urlfragment" -> "",
        "refr_medium" -> "",
        "refr_source" -> "",
        "refr_term" -> "",
        "mkt_medium" -> "",
        "mkt_source" -> "",
        "mkt_term" -> "",
        "mkt_content" -> "",
        "mkt_campaign" -> "",
        "contexts" -> "",
        "se_category" -> "",
        "se_action" -> "",
        "se_label" -> "",
        "se_property" -> "",
        "se_value" -> "",
        "unstruct_event" -> "",
        "tr_orderid" -> "",
        "tr_affiliation" -> "",
        "tr_total" -> "",
        "tr_tax" -> "",
        "tr_shipping" -> "",
        "tr_city" -> "",
        "tr_state" -> "",
        "tr_country" -> "",
        "ti_orderid" -> "",
        "ti_sku" -> "",
        "ti_name" -> "",
        "ti_category" -> "",
        "ti_price" -> "",
        "ti_quantity" -> "",
        "pp_xoffset_min" -> "",
        "pp_xoffset_max" -> "",
        "pp_yoffset_min" -> "",
        "pp_yoffset_max" -> "",
        "useragent" -> "",
        "br_name" -> "",
        "br_family" -> "",
        "br_version" -> "",
        "br_type" -> "",
        "br_renderengine" -> "",
        "br_lang" -> "",
        "br_features_pdf" -> "1",
        "br_features_flash" -> "0",
        "br_features_java" -> "",
        "br_features_director" -> "",
        "br_features_quicktime" -> "",
        "br_features_realplayer" -> "",
        "br_features_windowsmedia" -> "",
        "br_features_gears" -> "",
        "br_features_silverlight" -> "",
        "br_cookies" -> "",
        "br_colordepth" -> "",
        "br_viewwidth" -> "",
        "br_viewheight" -> "",
        "os_name" -> "",
        "os_family" -> "",
        "os_manufacturer" -> "",
        "os_timezone" -> "",
        "dvce_type" -> "",
        "dvce_ismobile" -> "",
        "dvce_screenwidth" -> "",
        "dvce_screenheight" -> "",
        "doc_charset" -> "",
        "doc_width" -> "",
        "doc_height" -> "",
        "tr_currency" -> "",
        "tr_total_base" -> "",
        "tr_tax_base" -> "",
        "tr_shipping_base" -> "",
        "ti_currency" -> "",
        "ti_price_base" -> "",
        "base_currency" -> "",
        "geo_timezone" -> "",
        "mkt_clickid" -> "",
        "mkt_network" -> "",
        "etl_tags" -> "",
        "dvce_sent_tstamp" -> "",
        "refr_domain_userid" -> "",
        "refr_device_tstamp" -> "",
        "derived_contexts" -> "",
        "domain_sessionid" -> "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1",
        "derived_tstamp" -> "2013-11-26 00:03:57.886",
        "event_vendor" -> "com.snowplowanalytics.snowplow",
        "event_name" -> "link_click",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event_fingerprint" -> "e3dbfa9cca0412c3d4052863cefb547f",
        "true_tstamp" -> "2013-11-26 00:03:57.886"
      )

      val expected = Event(
        app_id = Some("angry-birds"),
        platform = Some("web"),
        etl_tstamp = Some(Instant.parse("2017-01-26T00:01:25.292Z")),
        collector_tstamp = Instant.parse("2013-11-26T00:02:05Z"),
        dvce_created_tstamp = Some(Instant.parse("2013-11-26T00:03:57.885Z")),
        event = Some("page_view"),
        event_id = UUID.fromString("c6ef3124-b53a-4b13-a233-0088f79dcbcb"),
        txn_id = Some(41828),
        name_tracker = Some("cloudfront-1"),
        v_tracker = Some("js-2.1.0"),
        v_collector = "clj-tomcat-0.1.0",
        v_etl = "serde-0.5.2",
        user_id = Some("jon.doe@email.com"),
        user_ipaddress = Some("92.231.54.234"),
        user_fingerprint = Some("2161814971"),
        domain_userid = Some("bc2e92ec6c204a14"),
        domain_sessionidx = Some(3),
        network_userid = Some("ecdff4d0-9175-40ac-a8bb-325c49733607"),
        geo_country = Some("US"),
        geo_region = Some("TX"),
        geo_city = Some("New York"),
        geo_zipcode = Some("94109"),
        geo_latitude = Some(37.443604),
        geo_longitude = Some(-122.4124),
        geo_region_name = Some("Florida"),
        ip_isp = Some("FDN Communications"),
        ip_organization = Some("Bouygues Telecom"),
        ip_domain = Some("nuvox.net"),
        ip_netspeed = Some("Cable/DSL"),
        page_url = Some("http://www.snowplowanalytics.com"),
        page_title = Some("On Analytics"),
        page_referrer = None,
        page_urlscheme = Some("http"),
        page_urlhost = Some("www.snowplowanalytics.com"),
        page_urlport = Some(80),
        page_urlpath = Some("/product/index.html"),
        page_urlquery = Some("id=GTM-DLRG"),
        page_urlfragment = Some("4-conclusion"),
        refr_urlscheme = None,
        refr_urlhost = None,
        refr_urlport = None,
        refr_urlpath = None,
        refr_urlquery = None,
        refr_urlfragment = None,
        refr_medium = None,
        refr_source = None,
        refr_term = None,
        mkt_medium = None,
        mkt_source = None,
        mkt_term = None,
        mkt_content = None,
        mkt_campaign = None,
        contexts = Contexts(List()),
        se_category = None,
        se_action = None,
        se_label = None,
        se_property = None,
        se_value = None,
        unstruct_event = UnstructEvent(None),
        tr_orderid = None,
        tr_affiliation = None,
        tr_total = None,
        tr_tax = None,
        tr_shipping = None,
        tr_city = None,
        tr_state = None,
        tr_country = None,
        ti_orderid = None,
        ti_sku = None,
        ti_name = None,
        ti_category = None,
        ti_price = None,
        ti_quantity = None,
        pp_xoffset_min = None,
        pp_xoffset_max = None,
        pp_yoffset_min = None,
        pp_yoffset_max = None,
        useragent = None,
        br_name = None,
        br_family = None,
        br_version = None,
        br_type = None,
        br_renderengine = None,
        br_lang = None,
        br_features_pdf = Some(true),
        br_features_flash = Some(false),
        br_features_java = None,
        br_features_director = None,
        br_features_quicktime = None,
        br_features_realplayer = None,
        br_features_windowsmedia = None,
        br_features_gears = None,
        br_features_silverlight = None,
        br_cookies = None,
        br_colordepth = None,
        br_viewwidth = None,
        br_viewheight = None,
        os_name = None,
        os_family = None,
        os_manufacturer = None,
        os_timezone = None,
        dvce_type = None,
        dvce_ismobile = None,
        dvce_screenwidth = None,
        dvce_screenheight = None,
        doc_charset = None,
        doc_width = None,
        doc_height = None,
        tr_currency = None,
        tr_total_base = None,
        tr_tax_base = None,
        tr_shipping_base = None,
        ti_currency = None,
        ti_price_base = None,
        base_currency = None,
        geo_timezone = None,
        mkt_clickid = None,
        mkt_network = None,
        etl_tags = None,
        dvce_sent_tstamp = None,
        refr_domain_userid = None,
        refr_device_tstamp = None,
        derived_contexts = Contexts(List()),
        domain_sessionid = Some("2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1"),
        derived_tstamp = Some(Instant.parse("2013-11-26T00:03:57.886Z")),
        event_vendor = Some("com.snowplowanalytics.snowplow"),
        event_name = Some("link_click"),
        event_format = Some("jsonschema"),
        event_version = Some("1-0-0"),
        event_fingerprint = Some("e3dbfa9cca0412c3d4052863cefb547f"),
        true_tstamp = Some(Instant.parse("2013-11-26T00:03:57.886Z"))
      )

      val eventValues = input.unzip._2.mkString("\t")
      val event = Event.parse(eventValues)

      // Case class must be processed as expected
      event mustEqual Valid(expected)

      val eventJson = event.getOrElse(throw new RuntimeException("Failed to parse event")).toJson(true)

      val legacyJson = parse(EventTransformer
        .transform(eventValues)
        .right
        .toOption
        .getOrElse(throw new RuntimeException("Event failed transformation")))
          .right
          .toOption
          .getOrElse(throw new RuntimeException("Event failed transformation"))

      // JSON output must be equal to output from the old transformer. (NB: field ordering in new JSON will be randomized)
      eventJson mustEqual legacyJson
    }

    "successfully merge two matching contexts into 2-elements array" in {

      val input = List(
        "app_id" -> "angry-birds",
        "platform" -> "web",
        "etl_tstamp" -> "2017-01-26 00:01:25.292",
        "collector_tstamp" -> "2013-11-26 00:02:05",
        "dvce_created_tstamp" -> "2013-11-26 00:03:57.885",
        "event" -> "page_view",
        "event_id" -> "c6ef3124-b53a-4b13-a233-0088f79dcbcb",
        "txn_id" -> "41828",
        "name_tracker" -> "cloudfront-1",
        "v_tracker" -> "js-2.1.0",
        "v_collector" -> "clj-tomcat-0.1.0",
        "v_etl" -> "serde-0.5.2",
        "user_id" -> "jon.doe@email.com",
        "user_ipaddress" -> "92.231.54.234",
        "user_fingerprint" -> "2161814971",
        "domain_userid" -> "bc2e92ec6c204a14",
        "domain_sessionidx" -> "3",
        "network_userid" -> "ecdff4d0-9175-40ac-a8bb-325c49733607",
        "geo_country" -> "US",
        "geo_region" -> "TX",
        "geo_city" -> "New York",
        "geo_zipcode" -> "94109",
        "geo_latitude" -> "37.443604",
        "geo_longitude" -> "-122.4124",
        "geo_region_name" -> "Florida",
        "ip_isp" -> "FDN Communications",
        "ip_organization" -> "Bouygues Telecom",
        "ip_domain" -> "nuvox.net",
        "ip_netspeed" -> "Cable/DSL",
        "page_url" -> "http://www.snowplowanalytics.com",
        "page_title" -> "On Analytics",
        "page_referrer" -> "",
        "page_urlscheme" -> "http",
        "page_urlhost" -> "www.snowplowanalytics.com",
        "page_urlport" -> "80",
        "page_urlpath" -> "/product/index.html",
        "page_urlquery" -> "id=GTM-DLRG",
        "page_urlfragment" -> "4-conclusion",
        "refr_urlscheme" -> "",
        "refr_urlhost" -> "",
        "refr_urlport" -> "",
        "refr_urlpath" -> "",
        "refr_urlquery" -> "",
        "refr_urlfragment" -> "",
        "refr_medium" -> "",
        "refr_source" -> "",
        "refr_term" -> "",
        "mkt_medium" -> "",
        "mkt_source" -> "",
        "mkt_term" -> "",
        "mkt_content" -> "",
        "mkt_campaign" -> "",
        "contexts" -> contextsWithDuplicate,
        "se_category" -> "",
        "se_action" -> "",
        "se_label" -> "",
        "se_property" -> "",
        "se_value" -> "",
        "unstruct_event" -> unstructJson,
        "tr_orderid" -> "",
        "tr_affiliation" -> "",
        "tr_total" -> "",
        "tr_tax" -> "",
        "tr_shipping" -> "",
        "tr_city" -> "",
        "tr_state" -> "",
        "tr_country" -> "",
        "ti_orderid" -> "",
        "ti_sku" -> "",
        "ti_name" -> "",
        "ti_category" -> "",
        "ti_price" -> "",
        "ti_quantity" -> "",
        "pp_xoffset_min" -> "",
        "pp_xoffset_max" -> "",
        "pp_yoffset_min" -> "",
        "pp_yoffset_max" -> "",
        "useragent" -> "",
        "br_name" -> "",
        "br_family" -> "",
        "br_version" -> "",
        "br_type" -> "",
        "br_renderengine" -> "",
        "br_lang" -> "",
        "br_features_pdf" -> "1",
        "br_features_flash" -> "0",
        "br_features_java" -> "",
        "br_features_director" -> "",
        "br_features_quicktime" -> "",
        "br_features_realplayer" -> "",
        "br_features_windowsmedia" -> "",
        "br_features_gears" -> "",
        "br_features_silverlight" -> "",
        "br_cookies" -> "",
        "br_colordepth" -> "",
        "br_viewwidth" -> "",
        "br_viewheight" -> "",
        "os_name" -> "",
        "os_family" -> "",
        "os_manufacturer" -> "",
        "os_timezone" -> "",
        "dvce_type" -> "",
        "dvce_ismobile" -> "",
        "dvce_screenwidth" -> "",
        "dvce_screenheight" -> "",
        "doc_charset" -> "",
        "doc_width" -> "",
        "doc_height" -> "",
        "tr_currency" -> "",
        "tr_total_base" -> "",
        "tr_tax_base" -> "",
        "tr_shipping_base" -> "",
        "ti_currency" -> "",
        "ti_price_base" -> "",
        "base_currency" -> "",
        "geo_timezone" -> "",
        "mkt_clickid" -> "",
        "mkt_network" -> "",
        "etl_tags" -> "",
        "dvce_sent_tstamp" -> "",
        "refr_domain_userid" -> "",
        "refr_device_tstamp" -> "",
        "derived_contexts" -> derivedContextsJson,
        "domain_sessionid" -> "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1",
        "derived_tstamp" -> "2013-11-26 00:03:57.886",
        "event_vendor" -> "com.snowplowanalytics.snowplow",
        "event_name" -> "link_click",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event_fingerprint" -> "e3dbfa9cca0412c3d4052863cefb547f",
        "true_tstamp" -> "2013-11-26 00:03:57.886"
      )

      val expected = Event(
        app_id = Some("angry-birds"),
        platform = Some("web"),
        etl_tstamp = Some(Instant.parse("2017-01-26T00:01:25.292Z")),
        collector_tstamp = Instant.parse("2013-11-26T00:02:05Z"),
        dvce_created_tstamp = Some(Instant.parse("2013-11-26T00:03:57.885Z")),
        event = Some("page_view"),
        event_id = UUID.fromString("c6ef3124-b53a-4b13-a233-0088f79dcbcb"),
        txn_id = Some(41828),
        name_tracker = Some("cloudfront-1"),
        v_tracker = Some("js-2.1.0"),
        v_collector = "clj-tomcat-0.1.0",
        v_etl = "serde-0.5.2",
        user_id = Some("jon.doe@email.com"),
        user_ipaddress = Some("92.231.54.234"),
        user_fingerprint = Some("2161814971"),
        domain_userid = Some("bc2e92ec6c204a14"),
        domain_sessionidx = Some(3),
        network_userid = Some("ecdff4d0-9175-40ac-a8bb-325c49733607"),
        geo_country = Some("US"),
        geo_region = Some("TX"),
        geo_city = Some("New York"),
        geo_zipcode = Some("94109"),
        geo_latitude = Some(37.443604),
        geo_longitude = Some(-122.4124),
        geo_region_name = Some("Florida"),
        ip_isp = Some("FDN Communications"),
        ip_organization = Some("Bouygues Telecom"),
        ip_domain = Some("nuvox.net"),
        ip_netspeed = Some("Cable/DSL"),
        page_url = Some("http://www.snowplowanalytics.com"),
        page_title = Some("On Analytics"),
        page_referrer = None,
        page_urlscheme = Some("http"),
        page_urlhost = Some("www.snowplowanalytics.com"),
        page_urlport = Some(80),
        page_urlpath = Some("/product/index.html"),
        page_urlquery = Some("id=GTM-DLRG"),
        page_urlfragment = Some("4-conclusion"),
        refr_urlscheme = None,
        refr_urlhost = None,
        refr_urlport = None,
        refr_urlpath = None,
        refr_urlquery = None,
        refr_urlfragment = None,
        refr_medium = None,
        refr_source = None,
        refr_term = None,
        mkt_medium = None,
        mkt_source = None,
        mkt_term = None,
        mkt_content = None,
        mkt_campaign = None,
        contexts = Contexts(
          List(
            SelfDescribingData(
              SchemaKey(
                "org.schema",
                "WebPage",
                "jsonschema",
                SchemaVer.Full(1, 0, 0)
              ),
              JsonObject(
                ("genre", "blog".asJson),
                ("inLanguage", "en-US".asJson),
                ("datePublished", "2014-11-06T00:00:00Z".asJson),
                ("author", "Fred Blundun".asJson),
                ("breadcrumb", List("blog", "releases").asJson),
                ("keywords", List("snowplow", "javascript", "tracker", "event").asJson)
              ).asJson
            ),
            SelfDescribingData(
              SchemaKey(
                "org.acme",
                "context_one",
                "jsonschema",
                SchemaVer.Full(1, 0, 0)
              ),
              JsonObject(
                ("item", 1.asJson)
              ).asJson
            ),
            SelfDescribingData(
              SchemaKey(
                "org.acme",
                "context_one",
                "jsonschema",
                SchemaVer.Full(1, 0, 1)
              ),
              JsonObject(
                ("item", 2.asJson)
              ).asJson
            )
          )
        ),
        se_category = None,
        se_action = None,
        se_label = None,
        se_property = None,
        se_value = None,
        unstruct_event = UnstructEvent(
          Some(
            SelfDescribingData(
              SchemaKey(
                "com.snowplowanalytics.snowplow",
                "link_click",
                "jsonschema",
                SchemaVer.Full(1, 0, 1)
              ),
              JsonObject(
                ("targetUrl", "http://www.example.com".asJson),
                ("elementClasses", List("foreground").asJson),
                ("elementId", "exampleLink".asJson)
              ).asJson
            )
          )
        ),
        tr_orderid = None,
        tr_affiliation = None,
        tr_total = None,
        tr_tax = None,
        tr_shipping = None,
        tr_city = None,
        tr_state = None,
        tr_country = None,
        ti_orderid = None,
        ti_sku = None,
        ti_name = None,
        ti_category = None,
        ti_price = None,
        ti_quantity = None,
        pp_xoffset_min = None,
        pp_xoffset_max = None,
        pp_yoffset_min = None,
        pp_yoffset_max = None,
        useragent = None,
        br_name = None,
        br_family = None,
        br_version = None,
        br_type = None,
        br_renderengine = None,
        br_lang = None,
        br_features_pdf = Some(true),
        br_features_flash = Some(false),
        br_features_java = None,
        br_features_director = None,
        br_features_quicktime = None,
        br_features_realplayer = None,
        br_features_windowsmedia = None,
        br_features_gears = None,
        br_features_silverlight = None,
        br_cookies = None,
        br_colordepth = None,
        br_viewwidth = None,
        br_viewheight = None,
        os_name = None,
        os_family = None,
        os_manufacturer = None,
        os_timezone = None,
        dvce_type = None,
        dvce_ismobile = None,
        dvce_screenwidth = None,
        dvce_screenheight = None,
        doc_charset = None,
        doc_width = None,
        doc_height = None,
        tr_currency = None,
        tr_total_base = None,
        tr_tax_base = None,
        tr_shipping_base = None,
        ti_currency = None,
        ti_price_base = None,
        base_currency = None,
        geo_timezone = None,
        mkt_clickid = None,
        mkt_network = None,
        etl_tags = None,
        dvce_sent_tstamp = None,
        refr_domain_userid = None,
        refr_device_tstamp = None,
        derived_contexts = Contexts(
          List(
            SelfDescribingData(
              SchemaKey(
                "com.snowplowanalytics.snowplow",
                "ua_parser_context",
                "jsonschema",
                SchemaVer.Full(1, 0, 0)
              ),
              JsonObject(
                ("useragentFamily", "IE".asJson),
                ("useragentMajor", "7".asJson),
                ("useragentMinor", "0".asJson),
                ("useragentPatch", Json.Null),
                ("useragentVersion", "IE 7.0".asJson),
                ("osFamily", "Windows XP".asJson),
                ("osMajor", Json.Null),
                ("osMinor", Json.Null),
                ("osPatch", Json.Null),
                ("osPatchMinor", Json.Null),
                ("osVersion", "Windows XP".asJson),
                ("deviceFamily", "Other".asJson)
              ).asJson
            )
          )
        ),
        domain_sessionid = Some("2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1"),
        derived_tstamp = Some(Instant.parse("2013-11-26T00:03:57.886Z")),
        event_vendor = Some("com.snowplowanalytics.snowplow"),
        event_name = Some("link_click"),
        event_format = Some("jsonschema"),
        event_version = Some("1-0-0"),
        event_fingerprint = Some("e3dbfa9cca0412c3d4052863cefb547f"),
        true_tstamp = Some(Instant.parse("2013-11-26T00:03:57.886Z"))
      )

      val eventValues = input.unzip._2.mkString("\t")
      val event = Event.parse(eventValues)

      // Case class must be processed as expected
      event mustEqual Valid(expected)

      val eventJson = event.getOrElse(throw new RuntimeException("Failed to parse event")).toJson(true)

      val legacyJson = parse(EventTransformer
        .transform(eventValues)
        .right
        .toOption
        .getOrElse(throw new RuntimeException("Event failed transformation")))
          .right
          .toOption
          .getOrElse(throw new RuntimeException("Event failed transformation"))

      // JSON output must be equal to output from the old transformer. (NB: field ordering in new JSON will be randomized)
      eventJson mustEqual legacyJson
    }

    "return correct results from helper methods" in {
      val input = List(
        "app_id" -> "angry-birds",
        "platform" -> "web",
        "etl_tstamp" -> "2017-01-26 00:01:25.292",
        "collector_tstamp" -> "2013-11-26 00:02:05",
        "dvce_created_tstamp" -> "2013-11-26 00:03:57.885",
        "event" -> "page_view",
        "event_id" -> "c6ef3124-b53a-4b13-a233-0088f79dcbcb",
        "txn_id" -> "41828",
        "name_tracker" -> "cloudfront-1",
        "v_tracker" -> "js-2.1.0",
        "v_collector" -> "clj-tomcat-0.1.0",
        "v_etl" -> "serde-0.5.2",
        "user_id" -> "jon.doe@email.com",
        "user_ipaddress" -> "92.231.54.234",
        "user_fingerprint" -> "2161814971",
        "domain_userid" -> "bc2e92ec6c204a14",
        "domain_sessionidx" -> "3",
        "network_userid" -> "ecdff4d0-9175-40ac-a8bb-325c49733607",
        "geo_country" -> "US",
        "geo_region" -> "TX",
        "geo_city" -> "New York",
        "geo_zipcode" -> "94109",
        "geo_latitude" -> "37.443604",
        "geo_longitude" -> "-122.4124",
        "geo_region_name" -> "Florida",
        "ip_isp" -> "FDN Communications",
        "ip_organization" -> "Bouygues Telecom",
        "ip_domain" -> "nuvox.net",
        "ip_netspeed" -> "Cable/DSL",
        "page_url" -> "http://www.snowplowanalytics.com",
        "page_title" -> "On Analytics",
        "page_referrer" -> "",
        "page_urlscheme" -> "http",
        "page_urlhost" -> "www.snowplowanalytics.com",
        "page_urlport" -> "80",
        "page_urlpath" -> "/product/index.html",
        "page_urlquery" -> "id=GTM-DLRG",
        "page_urlfragment" -> "4-conclusion",
        "refr_urlscheme" -> "",
        "refr_urlhost" -> "",
        "refr_urlport" -> "",
        "refr_urlpath" -> "",
        "refr_urlquery" -> "",
        "refr_urlfragment" -> "",
        "refr_medium" -> "",
        "refr_source" -> "",
        "refr_term" -> "",
        "mkt_medium" -> "",
        "mkt_source" -> "",
        "mkt_term" -> "",
        "mkt_content" -> "",
        "mkt_campaign" -> "",
        "contexts" -> contextsWithDuplicate,
        "se_category" -> "",
        "se_action" -> "",
        "se_label" -> "",
        "se_property" -> "",
        "se_value" -> "",
        "unstruct_event" -> unstructJson,
        "tr_orderid" -> "",
        "tr_affiliation" -> "",
        "tr_total" -> "",
        "tr_tax" -> "",
        "tr_shipping" -> "",
        "tr_city" -> "",
        "tr_state" -> "",
        "tr_country" -> "",
        "ti_orderid" -> "",
        "ti_sku" -> "",
        "ti_name" -> "",
        "ti_category" -> "",
        "ti_price" -> "",
        "ti_quantity" -> "",
        "pp_xoffset_min" -> "",
        "pp_xoffset_max" -> "",
        "pp_yoffset_min" -> "",
        "pp_yoffset_max" -> "",
        "useragent" -> "",
        "br_name" -> "",
        "br_family" -> "",
        "br_version" -> "",
        "br_type" -> "",
        "br_renderengine" -> "",
        "br_lang" -> "",
        "br_features_pdf" -> "1",
        "br_features_flash" -> "0",
        "br_features_java" -> "",
        "br_features_director" -> "",
        "br_features_quicktime" -> "",
        "br_features_realplayer" -> "",
        "br_features_windowsmedia" -> "",
        "br_features_gears" -> "",
        "br_features_silverlight" -> "",
        "br_cookies" -> "",
        "br_colordepth" -> "",
        "br_viewwidth" -> "",
        "br_viewheight" -> "",
        "os_name" -> "",
        "os_family" -> "",
        "os_manufacturer" -> "",
        "os_timezone" -> "",
        "dvce_type" -> "",
        "dvce_ismobile" -> "",
        "dvce_screenwidth" -> "",
        "dvce_screenheight" -> "",
        "doc_charset" -> "",
        "doc_width" -> "",
        "doc_height" -> "",
        "tr_currency" -> "",
        "tr_total_base" -> "",
        "tr_tax_base" -> "",
        "tr_shipping_base" -> "",
        "ti_currency" -> "",
        "ti_price_base" -> "",
        "base_currency" -> "",
        "geo_timezone" -> "",
        "mkt_clickid" -> "",
        "mkt_network" -> "",
        "etl_tags" -> "",
        "dvce_sent_tstamp" -> "",
        "refr_domain_userid" -> "",
        "refr_device_tstamp" -> "",
        "derived_contexts" -> derivedContextsJson,
        "domain_sessionid" -> "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1",
        "derived_tstamp" -> "2013-11-26 00:03:57.886",
        "event_vendor" -> "com.snowplowanalytics.snowplow",
        "event_name" -> "link_click",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event_fingerprint" -> "e3dbfa9cca0412c3d4052863cefb547f",
        "true_tstamp" -> "2013-11-26 00:03:57.886"
      )

      val eventValues = input.unzip._2.mkString("\t")
      val event = Event.parse(eventValues).getOrElse(throw new RuntimeException("Failed to parse event"))

      event.geoLocation must beSome(("geo_location", "37.443604,-122.4124".asJson))
      event.contexts.toShreddedJson mustEqual Map(
        "contexts_org_schema_web_page_1" ->
          List(
            JsonObject(
              ("genre", "blog".asJson),
              ("inLanguage", "en-US".asJson),
              ("datePublished", "2014-11-06T00:00:00Z".asJson),
              ("author", "Fred Blundun".asJson),
              ("breadcrumb", List("blog", "releases").asJson),
              ("keywords", List("snowplow", "javascript", "tracker", "event").asJson)
            ).asJson
          ).asJson,
        "contexts_org_acme_context_one_1" ->
          List(
            JsonObject(
              ("item", 1.asJson)
            ).asJson,
            JsonObject(
              ("item", 2.asJson)
            ).asJson
          ).asJson
      )
      event.derived_contexts.toShreddedJson mustEqual Map(
        "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1" ->
          List(
            JsonObject(
              ("useragentFamily", "IE".asJson),
              ("useragentMajor", "7".asJson),
              ("useragentMinor", "0".asJson),
              ("useragentPatch", Json.Null),
              ("useragentVersion", "IE 7.0".asJson),
              ("osFamily", "Windows XP".asJson),
              ("osMajor", Json.Null),
              ("osMinor", Json.Null),
              ("osPatch", Json.Null),
              ("osPatchMinor", Json.Null),
              ("osVersion", "Windows XP".asJson),
              ("deviceFamily", "Other".asJson)
            ).asJson
          ).asJson
      )
      event.unstruct_event.toShreddedJson must beSome(
        "unstruct_event_com_snowplowanalytics_snowplow_link_click_1",
        JsonObject(
          ("targetUrl", "http://www.example.com".asJson),
          ("elementClasses", List("foreground").asJson),
          ("elementId", "exampleLink".asJson)
        ).asJson
      )
    }

    "fail if column values are invalid (and combine errors)" in {

      val input = List(
        "app_id" -> "angry-birds",
        "platform" -> "web",
        "etl_tstamp" -> "not_an_instant",
        "collector_tstamp" -> "",
        "dvce_created_tstamp" -> "2013-11-26 00:03:57.885",
        "event" -> "page_view",
        "event_id" -> "not_a_uuid",
        "txn_id" -> "not_an_integer",
        "name_tracker" -> "cloudfront-1",
        "v_tracker" -> "js-2.1.0",
        "v_collector" -> "",
        "v_etl" -> "serde-0.5.2",
        "user_id" -> "jon.doe@email.com",
        "user_ipaddress" -> "92.231.54.234",
        "user_fingerprint" -> "2161814971",
        "domain_userid" -> "bc2e92ec6c204a14",
        "domain_sessionidx" -> "3",
        "network_userid" -> "ecdff4d0-9175-40ac-a8bb-325c49733607",
        "geo_country" -> "US",
        "geo_region" -> "TX",
        "geo_city" -> "New York",
        "geo_zipcode" -> "94109",
        "geo_latitude" -> "not_a_double",
        "geo_longitude" -> "-122.4124",
        "geo_region_name" -> "Florida",
        "ip_isp" -> "FDN Communications",
        "ip_organization" -> "Bouygues Telecom",
        "ip_domain" -> "nuvox.net",
        "ip_netspeed" -> "Cable/DSL",
        "page_url" -> "http://www.snowplowanalytics.com",
        "page_title" -> "On Analytics",
        "page_referrer" -> "",
        "page_urlscheme" -> "http",
        "page_urlhost" -> "www.snowplowanalytics.com",
        "page_urlport" -> "80",
        "page_urlpath" -> "/product/index.html",
        "page_urlquery" -> "id=GTM-DLRG",
        "page_urlfragment" -> "4-conclusion",
        "refr_urlscheme" -> "",
        "refr_urlhost" -> "",
        "refr_urlport" -> "",
        "refr_urlpath" -> "",
        "refr_urlquery" -> "",
        "refr_urlfragment" -> "",
        "refr_medium" -> "",
        "refr_source" -> "",
        "refr_term" -> "",
        "mkt_medium" -> "",
        "mkt_source" -> "",
        "mkt_term" -> "",
        "mkt_content" -> "",
        "mkt_campaign" -> "",
        "contexts" -> contextsJson,
        "se_category" -> "",
        "se_action" -> "",
        "se_label" -> "",
        "se_property" -> "",
        "se_value" -> "",
        "unstruct_event" -> unstructJson,
        "tr_orderid" -> "",
        "tr_affiliation" -> "",
        "tr_total" -> "",
        "tr_tax" -> "",
        "tr_shipping" -> "",
        "tr_city" -> "",
        "tr_state" -> "",
        "tr_country" -> "",
        "ti_orderid" -> "",
        "ti_sku" -> "",
        "ti_name" -> "",
        "ti_category" -> "",
        "ti_price" -> "",
        "ti_quantity" -> "",
        "pp_xoffset_min" -> "",
        "pp_xoffset_max" -> "",
        "pp_yoffset_min" -> "",
        "pp_yoffset_max" -> "",
        "useragent" -> "",
        "br_name" -> "",
        "br_family" -> "",
        "br_version" -> "",
        "br_type" -> "",
        "br_renderengine" -> "",
        "br_lang" -> "",
        "br_features_pdf" -> "not_a_boolean",
        "br_features_flash" -> "0",
        "br_features_java" -> "",
        "br_features_director" -> "",
        "br_features_quicktime" -> "",
        "br_features_realplayer" -> "",
        "br_features_windowsmedia" -> "",
        "br_features_gears" -> "",
        "br_features_silverlight" -> "",
        "br_cookies" -> "",
        "br_colordepth" -> "",
        "br_viewwidth" -> "",
        "br_viewheight" -> "",
        "os_name" -> "",
        "os_family" -> "",
        "os_manufacturer" -> "",
        "os_timezone" -> "",
        "dvce_type" -> "",
        "dvce_ismobile" -> "",
        "dvce_screenwidth" -> "",
        "dvce_screenheight" -> "",
        "doc_charset" -> "",
        "doc_width" -> "",
        "doc_height" -> "",
        "tr_currency" -> "",
        "tr_total_base" -> "",
        "tr_tax_base" -> "",
        "tr_shipping_base" -> "",
        "ti_currency" -> "",
        "ti_price_base" -> "",
        "base_currency" -> "",
        "geo_timezone" -> "",
        "mkt_clickid" -> "",
        "mkt_network" -> "",
        "etl_tags" -> "",
        "dvce_sent_tstamp" -> "",
        "refr_domain_userid" -> "",
        "refr_device_tstamp" -> "",
        "derived_contexts" -> derivedContextsJson,
        "domain_sessionid" -> "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1",
        "derived_tstamp" -> "2013-11-26 00:03:57.886",
        "event_vendor" -> "com.snowplowanalytics.snowplow",
        "event_name" -> "link_click",
        "event_format" -> "jsonschema",
        "event_version" -> "1-0-0",
        "event_fingerprint" -> "e3dbfa9cca0412c3d4052863cefb547f"
      )

      val eventValues = input.unzip._2.mkString("\t")
      val event = Event.parse(eventValues)

      // Case class must be correctly invalidated
      event mustEqual Invalid(NonEmptyList.of(
        "Cannot parse key 'etl_tstamp with value not_an_instant into datetime",
        "Field 'collector_tstamp cannot be empty",
        "Cannot parse key 'event_id with value not_a_uuid into UUID",
        "Cannot parse key 'txn_id with value not_an_integer into integer",
        "Field 'v_collector cannot be empty",
        "Cannot parse key 'geo_latitude with value not_a_double into double",
        "Cannot parse key 'br_features_pdf with value not_a_boolean into boolean",
        "Cannot parse key 'true_tstamp with value VALUE IS MISSING into datetime"
      ))
    }
  }

  "The transformSchema method" should {
    "successfully convert schemas into snake_case" in {
      SnowplowEvent.transformSchema(Data.Contexts(Data.CustomContexts), "org.w3", "PerformanceTiming", 1) mustEqual "contexts_org_w3_performance_timing_1"
      SnowplowEvent.transformSchema(Data.Contexts(Data.CustomContexts), "org.w3", "PerformanceTiming", 1) mustEqual Data.fixSchema(Data.Contexts(Data.CustomContexts), "iglu:org.w3/PerformanceTiming/jsonschema/1-0-0")
      SnowplowEvent.transformSchema(Data.Contexts(Data.CustomContexts), SchemaKey("org.w3", "PerformanceTiming", "jsonschema", SchemaVer.Full(1, 0, 0))) mustEqual "contexts_org_w3_performance_timing_1"
      SnowplowEvent.transformSchema(Data.Contexts(Data.CustomContexts), "com.snowplowanalytics.snowplow", "ua_parser_context", 1) mustEqual "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1"
      SnowplowEvent.transformSchema(Data.UnstructEvent, "com.snowplowanalytics.self-desc", "schema", 1) mustEqual "unstruct_event_com_snowplowanalytics_self_desc_schema_1"
    }
  }
}
