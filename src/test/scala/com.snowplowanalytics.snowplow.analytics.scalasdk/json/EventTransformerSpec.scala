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
import org.json4s.jackson.JsonMethods._

// Specs2
import org.specs2.mutable.Specification

// This library
import EventTransformer._
import Data._

/**
 * Tests SnowplowElasticsearchTransformer
 */
class EventTransformerSpec extends Specification {

  val unstructJson = """{
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

  val contextsJson = """{
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

  val derivedContextsJson = """{
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

  "The 'transform' method" should {
    "successfully convert a tab-separated unstructured event string to JSON" in {

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

      val expected = parse("""{
        "geo_location" : "37.443604,-122.4124",
        "app_id" : "angry-birds",
        "platform" : "web",
        "etl_tstamp" : "2017-01-26T00:01:25.292Z",
        "collector_tstamp" : "2013-11-26T00:02:05Z",
        "dvce_created_tstamp" : "2013-11-26T00:03:57.885Z",
        "event" : "page_view",
        "event_id" : "c6ef3124-b53a-4b13-a233-0088f79dcbcb",
        "txn_id" : 41828,
        "name_tracker" : "cloudfront-1",
        "v_tracker" : "js-2.1.0",
        "v_collector" : "clj-tomcat-0.1.0",
        "v_etl" : "serde-0.5.2",
        "user_id" : "jon.doe@email.com",
        "user_ipaddress" : "92.231.54.234",
        "user_fingerprint" : "2161814971",
        "domain_userid" : "bc2e92ec6c204a14",
        "domain_sessionidx" : 3,
        "network_userid" : "ecdff4d0-9175-40ac-a8bb-325c49733607",
        "geo_country" : "US",
        "geo_region" : "TX",
        "geo_city" : "New York",
        "geo_zipcode" : "94109",
        "geo_latitude" : 37.443604,
        "geo_longitude" : -122.4124,
        "geo_region_name" : "Florida",
        "ip_isp" : "FDN Communications",
        "ip_organization" : "Bouygues Telecom",
        "ip_domain" : "nuvox.net",
        "ip_netspeed" : "Cable/DSL",
        "page_url" : "http://www.snowplowanalytics.com",
        "page_title" : "On Analytics",
        "page_referrer" : null,
        "page_urlscheme" : "http",
        "page_urlhost" : "www.snowplowanalytics.com",
        "page_urlport" : 80,
        "page_urlpath" : "/product/index.html",
        "page_urlquery" : "id=GTM-DLRG",
        "page_urlfragment" : "4-conclusion",
        "refr_urlscheme" : null,
        "refr_urlhost" : null,
        "refr_urlport" : null,
        "refr_urlpath" : null,
        "refr_urlquery" : null,
        "refr_urlfragment" : null,
        "refr_medium" : null,
        "refr_source" : null,
        "refr_term" : null,
        "mkt_medium" : null,
        "mkt_source" : null,
        "mkt_term" : null,
        "mkt_content" : null,
        "mkt_campaign" : null,
        "contexts_org_schema_web_page_1" : [ {
          "genre" : "blog",
          "inLanguage" : "en-US",
          "datePublished" : "2014-11-06T00:00:00Z",
          "author" : "Fred Blundun",
          "breadcrumb" : [ "blog", "releases" ],
          "keywords" : [ "snowplow", "javascript", "tracker", "event" ]
        } ],
        "contexts_org_w3_performance_timing_1" : [ {
          "navigationStart" : 1415358089861,
          "unloadEventStart" : 1415358090270,
          "unloadEventEnd" : 1415358090287,
          "redirectStart" : 0,
          "redirectEnd" : 0,
          "fetchStart" : 1415358089870,
          "domainLookupStart" : 1415358090102,
          "domainLookupEnd" : 1415358090102,
          "connectStart" : 1415358090103,
          "connectEnd" : 1415358090183,
          "requestStart" : 1415358090183,
          "responseStart" : 1415358090265,
          "responseEnd" : 1415358090265,
          "domLoading" : 1415358090270,
          "domInteractive" : 1415358090886,
          "domContentLoadedEventStart" : 1415358090968,
          "domContentLoadedEventEnd" : 1415358091309,
          "domComplete" : 0,
          "loadEventStart" : 0,
          "loadEventEnd" : 0
        } ],
        "se_category" : null,
        "se_action" : null,
        "se_label" : null,
        "se_property" : null,
        "se_value" : null,
        "unstruct_event_com_snowplowanalytics_snowplow_link_click_1" : {
          "targetUrl" : "http://www.example.com",
          "elementClasses" : [ "foreground" ],
          "elementId" : "exampleLink"
        },
        "tr_orderid" : null,
        "tr_affiliation" : null,
        "tr_total" : null,
        "tr_tax" : null,
        "tr_shipping" : null,
        "tr_city" : null,
        "tr_state" : null,
        "tr_country" : null,
        "ti_orderid" : null,
        "ti_sku" : null,
        "ti_name" : null,
        "ti_category" : null,
        "ti_price" : null,
        "ti_quantity" : null,
        "pp_xoffset_min" : null,
        "pp_xoffset_max" : null,
        "pp_yoffset_min" : null,
        "pp_yoffset_max" : null,
        "useragent" : null,
        "br_name" : null,
        "br_family" : null,
        "br_version" : null,
        "br_type" : null,
        "br_renderengine" : null,
        "br_lang" : null,
        "br_features_pdf" : true,
        "br_features_flash" : false,
        "br_features_java" : null,
        "br_features_director" : null,
        "br_features_quicktime" : null,
        "br_features_realplayer" : null,
        "br_features_windowsmedia" : null,
        "br_features_gears" : null,
        "br_features_silverlight" : null,
        "br_cookies" : null,
        "br_colordepth" : null,
        "br_viewwidth" : null,
        "br_viewheight" : null,
        "os_name" : null,
        "os_family" : null,
        "os_manufacturer" : null,
        "os_timezone" : null,
        "dvce_type" : null,
        "dvce_ismobile" : null,
        "dvce_screenwidth" : null,
        "dvce_screenheight" : null,
        "doc_charset" : null,
        "doc_width" : null,
        "doc_height" : null,
        "tr_currency" : null,
        "tr_total_base" : null,
        "tr_tax_base" : null,
        "tr_shipping_base" : null,
        "ti_currency" : null,
        "ti_price_base" : null,
        "base_currency" : null,
        "geo_timezone" : null,
        "mkt_clickid" : null,
        "mkt_network" : null,
        "etl_tags" : null,
        "dvce_sent_tstamp" : null,
        "refr_domain_userid" : null,
        "refr_device_tstamp" : null,
        "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1": [{
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
        }],
        "domain_sessionid": "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1",
        "derived_tstamp": "2013-11-26T00:03:57.886Z",
        "event_vendor": "com.snowplowanalytics.snowplow",
        "event_name": "link_click",
        "event_format": "jsonschema",
        "event_version": "1-0-0",
        "event_fingerprint": "e3dbfa9cca0412c3d4052863cefb547f",
        "true_tstamp": "2013-11-26T00:03:57.886Z"
      }""")

      val eventValues = input.unzip._2.mkString("\t")

      val jsonStr = EventTransformer
        .transform(eventValues)
        .right
        .toOption
        .getOrElse(throw new RuntimeException("Event failed transformation"))

      val resultJson = parse(jsonStr)

      // Unstructured event shredding
      val elementIdExpectation = resultJson \ "unstruct_event_com_snowplowanalytics_snowplow_link_click_1" \ "elementId" must_== JString("exampleLink")

      // Contexts shredding
      val contextsExpectation = resultJson \ "contexts_org_schema_web_page_1" \ "genre" must_== JString("blog")

      // The entire JSON
      val diffExpectation = (resultJson diff expected) mustEqual Diff(JNothing, JNothing, JNothing)

      elementIdExpectation.and(contextsExpectation).and(diffExpectation)
    }

    "successfully convert a tab-separated page-view event string to JSON, omitting unstruct_event and contexts nullary fields" in {

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

      val eventValues = input.unzip._2.mkString("\t")

      val expected = parse("""{
        "geo_location" : "37.443604,-122.4124",
        "app_id" : "angry-birds",
        "platform" : "web",
        "etl_tstamp" : "2017-01-26T00:01:25.292Z",
        "collector_tstamp" : "2013-11-26T00:02:05Z",
        "dvce_created_tstamp" : "2013-11-26T00:03:57.885Z",
        "event" : "page_view",
        "event_id" : "c6ef3124-b53a-4b13-a233-0088f79dcbcb",
        "txn_id" : 41828,
        "name_tracker" : "cloudfront-1",
        "v_tracker" : "js-2.1.0",
        "v_collector" : "clj-tomcat-0.1.0",
        "v_etl" : "serde-0.5.2",
        "user_id" : "jon.doe@email.com",
        "user_ipaddress" : "92.231.54.234",
        "user_fingerprint" : "2161814971",
        "domain_userid" : "bc2e92ec6c204a14",
        "domain_sessionidx" : 3,
        "network_userid" : "ecdff4d0-9175-40ac-a8bb-325c49733607",
        "geo_country" : "US",
        "geo_region" : "TX",
        "geo_city" : "New York",
        "geo_zipcode" : "94109",
        "geo_latitude" : 37.443604,
        "geo_longitude" : -122.4124,
        "geo_region_name" : "Florida",
        "ip_isp" : "FDN Communications",
        "ip_organization" : "Bouygues Telecom",
        "ip_domain" : "nuvox.net",
        "ip_netspeed" : "Cable/DSL",
        "page_url" : "http://www.snowplowanalytics.com",
        "page_title" : "On Analytics",
        "page_referrer" : null,
        "page_urlscheme" : "http",
        "page_urlhost" : "www.snowplowanalytics.com",
        "page_urlport" : 80,
        "page_urlpath" : "/product/index.html",
        "page_urlquery" : "id=GTM-DLRG",
        "page_urlfragment" : "4-conclusion",
        "refr_urlscheme" : null,
        "refr_urlhost" : null,
        "refr_urlport" : null,
        "refr_urlpath" : null,
        "refr_urlquery" : null,
        "refr_urlfragment" : null,
        "refr_medium" : null,
        "refr_source" : null,
        "refr_term" : null,
        "mkt_medium" : null,
        "mkt_source" : null,
        "mkt_term" : null,
        "mkt_content" : null,
        "mkt_campaign" : null,
        "se_category" : null,
        "se_action" : null,
        "se_label" : null,
        "se_property" : null,
        "se_value" : null,
        "tr_orderid" : null,
        "tr_affiliation" : null,
        "tr_total" : null,
        "tr_tax" : null,
        "tr_shipping" : null,
        "tr_city" : null,
        "tr_state" : null,
        "tr_country" : null,
        "ti_orderid" : null,
        "ti_sku" : null,
        "ti_name" : null,
        "ti_category" : null,
        "ti_price" : null,
        "ti_quantity" : null,
        "pp_xoffset_min" : null,
        "pp_xoffset_max" : null,
        "pp_yoffset_min" : null,
        "pp_yoffset_max" : null,
        "useragent" : null,
        "br_name" : null,
        "br_family" : null,
        "br_version" : null,
        "br_type" : null,
        "br_renderengine" : null,
        "br_lang" : null,
        "br_features_pdf" : true,
        "br_features_flash" : false,
        "br_features_java" : null,
        "br_features_director" : null,
        "br_features_quicktime" : null,
        "br_features_realplayer" : null,
        "br_features_windowsmedia" : null,
        "br_features_gears" : null,
        "br_features_silverlight" : null,
        "br_cookies" : null,
        "br_colordepth" : null,
        "br_viewwidth" : null,
        "br_viewheight" : null,
        "os_name" : null,
        "os_family" : null,
        "os_manufacturer" : null,
        "os_timezone" : null,
        "dvce_type" : null,
        "dvce_ismobile" : null,
        "dvce_screenwidth" : null,
        "dvce_screenheight" : null,
        "doc_charset" : null,
        "doc_width" : null,
        "doc_height" : null,
        "tr_currency" : null,
        "tr_total_base" : null,
        "tr_tax_base" : null,
        "tr_shipping_base" : null,
        "ti_currency" : null,
        "ti_price_base" : null,
        "base_currency" : null,
        "geo_timezone" : null,
        "mkt_clickid" : null,
        "mkt_network" : null,
        "etl_tags" : null,
        "dvce_sent_tstamp" : null,
        "refr_domain_userid" : null,
        "refr_device_tstamp" : null,
        "domain_sessionid": "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1",
        "derived_tstamp": "2013-11-26T00:03:57.886Z",
        "event_vendor": "com.snowplowanalytics.snowplow",
        "event_name": "link_click",
        "event_format": "jsonschema",
        "event_version": "1-0-0",
        "event_fingerprint": "e3dbfa9cca0412c3d4052863cefb547f",
        "true_tstamp": "2013-11-26T00:03:57.886Z"
      }""")

      val jsonStr = EventTransformer
        .transform(eventValues)
        .right
        .toOption
        .getOrElse(throw new RuntimeException("Event failed transformation"))

      val resultJson = parse(jsonStr)

      // Unstructured event shredding
      val elementIdExpectation = resultJson \ "unstruct_event_com_snowplowanalytics_snowplow_link_click_1" \ "elementId" must_== JNothing
      val unstructEventNotNullExpectation = resultJson \ "unstruct_event" \ "elementId" must_== JNothing

      // Contexts shredding
      val contextsExpectation = resultJson \ "contexts_org_schema_web_page_1" must_== JNothing
      val contextsNotNullExpectation = resultJson \ "contexts" must_== JNothing

      // The entire JSON
      val diffExpectation = (resultJson diff expected) mustEqual Diff(JNothing, JNothing, JNothing)

      elementIdExpectation.and(unstructEventNotNullExpectation).and(contextsNotNullExpectation).and(contextsExpectation).and(diffExpectation)
    }

  }

  "The 'transformWithInventory' method" should {
    "successfully extract " in {

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

      val expected = parse("""{
        "geo_location" : "37.443604,-122.4124",
        "app_id" : "angry-birds",
        "platform" : "web",
        "etl_tstamp" : "2017-01-26T00:01:25.292Z",
        "collector_tstamp" : "2013-11-26T00:02:05Z",
        "dvce_created_tstamp" : "2013-11-26T00:03:57.885Z",
        "event" : "page_view",
        "event_id" : "c6ef3124-b53a-4b13-a233-0088f79dcbcb",
        "txn_id" : 41828,
        "name_tracker" : "cloudfront-1",
        "v_tracker" : "js-2.1.0",
        "v_collector" : "clj-tomcat-0.1.0",
        "v_etl" : "serde-0.5.2",
        "user_id" : "jon.doe@email.com",
        "user_ipaddress" : "92.231.54.234",
        "user_fingerprint" : "2161814971",
        "domain_userid" : "bc2e92ec6c204a14",
        "domain_sessionidx" : 3,
        "network_userid" : "ecdff4d0-9175-40ac-a8bb-325c49733607",
        "geo_country" : "US",
        "geo_region" : "TX",
        "geo_city" : "New York",
        "geo_zipcode" : "94109",
        "geo_latitude" : 37.443604,
        "geo_longitude" : -122.4124,
        "geo_region_name" : "Florida",
        "ip_isp" : "FDN Communications",
        "ip_organization" : "Bouygues Telecom",
        "ip_domain" : "nuvox.net",
        "ip_netspeed" : "Cable/DSL",
        "page_url" : "http://www.snowplowanalytics.com",
        "page_title" : "On Analytics",
        "page_referrer" : null,
        "page_urlscheme" : "http",
        "page_urlhost" : "www.snowplowanalytics.com",
        "page_urlport" : 80,
        "page_urlpath" : "/product/index.html",
        "page_urlquery" : "id=GTM-DLRG",
        "page_urlfragment" : "4-conclusion",
        "refr_urlscheme" : null,
        "refr_urlhost" : null,
        "refr_urlport" : null,
        "refr_urlpath" : null,
        "refr_urlquery" : null,
        "refr_urlfragment" : null,
        "refr_medium" : null,
        "refr_source" : null,
        "refr_term" : null,
        "mkt_medium" : null,
        "mkt_source" : null,
        "mkt_term" : null,
        "mkt_content" : null,
        "mkt_campaign" : null,
        "contexts_org_schema_web_page_1" : [ {
          "genre" : "blog",
          "inLanguage" : "en-US",
          "datePublished" : "2014-11-06T00:00:00Z",
          "author" : "Fred Blundun",
          "breadcrumb" : [ "blog", "releases" ],
          "keywords" : [ "snowplow", "javascript", "tracker", "event" ]
        } ],
        "contexts_org_w3_performance_timing_1" : [ {
          "navigationStart" : 1415358089861,
          "unloadEventStart" : 1415358090270,
          "unloadEventEnd" : 1415358090287,
          "redirectStart" : 0,
          "redirectEnd" : 0,
          "fetchStart" : 1415358089870,
          "domainLookupStart" : 1415358090102,
          "domainLookupEnd" : 1415358090102,
          "connectStart" : 1415358090103,
          "connectEnd" : 1415358090183,
          "requestStart" : 1415358090183,
          "responseStart" : 1415358090265,
          "responseEnd" : 1415358090265,
          "domLoading" : 1415358090270,
          "domInteractive" : 1415358090886,
          "domContentLoadedEventStart" : 1415358090968,
          "domContentLoadedEventEnd" : 1415358091309,
          "domComplete" : 0,
          "loadEventStart" : 0,
          "loadEventEnd" : 0
        } ],
        "se_category" : null,
        "se_action" : null,
        "se_label" : null,
        "se_property" : null,
        "se_value" : null,
        "unstruct_event_com_snowplowanalytics_snowplow_link_click_1" : {
          "targetUrl" : "http://www.example.com",
          "elementClasses" : [ "foreground" ],
          "elementId" : "exampleLink"
        },
        "tr_orderid" : null,
        "tr_affiliation" : null,
        "tr_total" : null,
        "tr_tax" : null,
        "tr_shipping" : null,
        "tr_city" : null,
        "tr_state" : null,
        "tr_country" : null,
        "ti_orderid" : null,
        "ti_sku" : null,
        "ti_name" : null,
        "ti_category" : null,
        "ti_price" : null,
        "ti_quantity" : null,
        "pp_xoffset_min" : null,
        "pp_xoffset_max" : null,
        "pp_yoffset_min" : null,
        "pp_yoffset_max" : null,
        "useragent" : null,
        "br_name" : null,
        "br_family" : null,
        "br_version" : null,
        "br_type" : null,
        "br_renderengine" : null,
        "br_lang" : null,
        "br_features_pdf" : true,
        "br_features_flash" : false,
        "br_features_java" : null,
        "br_features_director" : null,
        "br_features_quicktime" : null,
        "br_features_realplayer" : null,
        "br_features_windowsmedia" : null,
        "br_features_gears" : null,
        "br_features_silverlight" : null,
        "br_cookies" : null,
        "br_colordepth" : null,
        "br_viewwidth" : null,
        "br_viewheight" : null,
        "os_name" : null,
        "os_family" : null,
        "os_manufacturer" : null,
        "os_timezone" : null,
        "dvce_type" : null,
        "dvce_ismobile" : null,
        "dvce_screenwidth" : null,
        "dvce_screenheight" : null,
        "doc_charset" : null,
        "doc_width" : null,
        "doc_height" : null,
        "tr_currency" : null,
        "tr_total_base" : null,
        "tr_tax_base" : null,
        "tr_shipping_base" : null,
        "ti_currency" : null,
        "ti_price_base" : null,
        "base_currency" : null,
        "geo_timezone" : null,
        "mkt_clickid" : null,
        "mkt_network" : null,
        "etl_tags" : null,
        "dvce_sent_tstamp" : null,
        "refr_domain_userid" : null,
        "refr_device_tstamp" : null,
        "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1": [{
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
        }],
        "domain_sessionid": "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1",
        "derived_tstamp": "2013-11-26T00:03:57.886Z",
        "event_vendor": "com.snowplowanalytics.snowplow",
        "event_name": "link_click",
        "event_format": "jsonschema",
        "event_version": "1-0-0",
        "event_fingerprint": "e3dbfa9cca0412c3d4052863cefb547f",
        "true_tstamp": "2013-11-26T00:03:57.886Z"
      }""")

      val eventValues = input.unzip._2.mkString("\t")

      val eventWithInventory = EventTransformer
        .transformWithInventory(eventValues)
        .right
        .toOption
        .getOrElse(throw new RuntimeException("Event failed transformation"))

      val resultJson = parse(eventWithInventory.event)

      val inventoryExpectation = eventWithInventory.inventory mustEqual(Set(
        InventoryItemOld(Contexts(CustomContexts), "iglu:org.schema/WebPage/jsonschema/1-0-0"),
        InventoryItemOld(Contexts(CustomContexts), "iglu:org.w3/PerformanceTiming/jsonschema/1-0-0"),
        InventoryItemOld(UnstructEvent, "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1"),
        InventoryItemOld(Contexts(DerivedContexts), "iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0")
      ))

      val diffExpectation = (resultJson diff expected) mustEqual Diff(JNothing, JNothing, JNothing)

      diffExpectation.and(inventoryExpectation)
    }
  }

  "The 'jsonifyGoodEvent' method" should {
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

      val (_, json) = EventTransformer
        .jsonifyGoodEvent(input.map(_._2).toArray)
        .right
        .toOption
        .getOrElse(throw new RuntimeException("Event failed transformation"))

      val contexts = json match {
        case obj: JObject => obj.obj.filter { case (key, _) => key == "contexts_org_acme_context_one_1" }
        case _ => Nil
      }

      // List[(Context, ContextCardinality)]
      val result = contexts.map { case (k, v) => v match {
        case a: JArray => (k, a.arr.length)
        case _ => (k, 0)
      } }

      result should beEqualTo(List("contexts_org_acme_context_one_1" -> 2))
    }
  }

  "The 'getValidatedJsonEvent' method" should {
    "successfully return the original event when called with flatten false" in {

      val latitude = "37.443604"
      val longitude = "-122.4124"
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
        "geo_latitude" -> latitude,
        "geo_longitude" -> longitude,
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

      val (inventory, json) = EventTransformer
        .getValidatedJsonEvent(input.map(_._2).toArray, false)
        .right
        .toOption
        .getOrElse(throw new RuntimeException("Event failed transformation"))

      val expectedContexts = parse(contextsWithDuplicate)

      inventory mustEqual Set(
        InventoryItemOld(Contexts(CustomContexts), "iglu:org.schema/WebPage/jsonschema/1-0-0"),
        InventoryItemOld(UnstructEvent, "iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1"),
        InventoryItemOld(Contexts(DerivedContexts), "iglu:com.snowplowanalytics.snowplow/ua_parser_context/jsonschema/1-0-0"),
        InventoryItemOld(Contexts(CustomContexts),"iglu:org.acme/context_one/jsonschema/1-0-1"),
        InventoryItemOld(Contexts(CustomContexts),"iglu:org.acme/context_one/jsonschema/1-0-0")
      )

      val contexts = json \ "contexts"
      val contextsWithName = contexts aka "contexts"
      contextsWithName should haveClass[JObject]
      contextsWithName should be equalTo expectedContexts

      val derivedContexts = json \ "derived_contexts"
      val derivedContextsWithName = derivedContexts aka "derived_ontexts"
      derivedContextsWithName should haveClass[JObject]
      derivedContextsWithName should be equalTo(parse(derivedContextsJson))

      val unstructEvent = json \ "unstruct_event"
      val unstructWithName = unstructEvent aka "unstruct_event"
      unstructWithName should haveClass[JObject]
      unstructWithName should be equalTo(parse(unstructJson))

      val expectedOutput = input :+ ("geo_location" -> s"$latitude,$longitude")
      val expectedKeys = expectedOutput.map(_._1)
      json.values.keys.toSet aka "returned keys" should be equalTo(expectedKeys.toSet)

      implicit val formats = DefaultFormats

      val expectedValues = expectedOutput.sortBy(_._1).map(_._2).map(s => if (s.startsWith("201")) s.replace(" ", "T")+"Z" else s).map(v => if (v == "") null else v)
      val returnedValues = json.obj.sortBy(_._1).map{ case (_, v: JValue) => v}
      val stringValues = returnedValues.map(_.extractOpt[String])
      val stringIndexes = stringValues.zipWithIndex.map { case (o: Option[String], i: Int) => o.map(_ => i)}.flatten
      val returnedStrings = stringValues.flatten
      returnedStrings aka "returned values" should be equalTo(stringIndexes.map(expectedValues))
    }

    "return a list of errors for invalid input" in {

      val latitude = "37.443604"
      val longitude = "-122.4124"
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
        "domain_sessionidx" -> "3a",
        "network_userid" -> "ecdff4d0-9175-40ac-a8bb-325c49733607",
        "geo_country" -> "US",
        "geo_region" -> "TX",
        "geo_city" -> "New York",
        "geo_zipcode" -> "94109",
        "geo_latitude" -> latitude,
        "geo_longitude" -> longitude,
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
        "br_features_java" -> "2",
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

      val errorList = EventTransformer
        .getValidatedJsonEvent(input.map(_._2).toArray, false)
        .left
        .toOption
          .getOrElse(throw new RuntimeException("Error validating event"))

      errorList should be equalTo(List("java.lang.NumberFormatException: For input string: \"3a\"", "com.snowplowanalytics.snowplow.analytics.scalasdk.json.BooleanFormatException: Invalid boolean value: 2"))
    }
  }

  "The 'foldContexts' function" should {
    "successfully merge three contexts" in {
      val input = parse(
        """
          |{
          |  "contexts_one_1": [{"value": 1}],
          |  "app_id": "foo",
          |  "contexts_one_1": [{"value": 2}],
          |  "contexts_one_1": [{"value": 3}]
          |}
        """.stripMargin).asInstanceOf[JObject]

      val expected = parse(
        """
          |{
          |  "app_id": "foo",
          |  "contexts_one_1": [{"value": 1}, {"value": 2}, {"value": 3}]
          |}
        """.stripMargin)

      val result = EventTransformer.foldContexts(input)
      result must beEqualTo(expected)
    }

    "not merge contexts with different models" in {
      val input = parse(
        """
          |{
          |  "app_id": "foo",
          |  "contexts_one_1": [{"value": 1}],
          |  "contexts_one_2": [{"value": 2}]
          |}
        """.stripMargin).asInstanceOf[JObject]

      val expected = parse(
        """
          |{
          |  "app_id": "foo",
          |  "contexts_one_1": [{"value": 1}],
          |  "contexts_one_2": [{"value": 2}]
          |}
        """.stripMargin)

      val result = EventTransformer.foldContexts(input)
      result must beEqualTo(expected)
    }

    "successfully merge contexts with more than one context" in {
      val input = parse(
        """
          |{
          |  "contexts_one_1": [{"value": 1}],
          |  "app_id": "foo",
          |  "contexts_one_1": [{"value": 2}, {"value": 2}],
          |  "contexts_one_1": [{"value": 3}, {"value": 4}]
          |}
        """.stripMargin).asInstanceOf[JObject]

      val expected = parse(
        """
          |{
          |  "app_id": "foo",
          |  "contexts_one_1": [{"value": 1}, {"value": 2}, {"value": 2}, {"value": 3}, {"value": 4}]
          |}
        """.stripMargin)

      val result = EventTransformer.foldContexts(input)
      result must beEqualTo(expected)
    }
  }

}
