# Snowplow Scala Analytics SDK

[![Build Status][travis-image]][travis]
[![Release][release-image]][releases]
[![License][license-image]][license]

## Overview

**[Snowplow][snowplow]** Analytics SDK for Scala lets you work with **[Snowplow enriched events][enriched-events]** in your Scala event processing and data modeling jobs.

Use this SDK with **[Apache Spark][spark]**, **[AWS Lambda][lambda]**, **[Apache Flink][flink]**, **[Scalding][scalding]**, **[Apache Samza][samza]** and other Scala/JVM-compatible data processing frameworks.

## Documentation

Setup guide and user guide can be found on our [documentation website](https://docs.snowplowanalytics.com/docs/modeling-your-data/analytics-sdk/analytics-sdk-scala/).

Scaladoc for this project can be found as Github pages [here][scala-doc].

## Benchmarking

This project comes with [sbt-jmh](https://github.com/ktoso/sbt-jmh).

Benchmarks need to be added [here](./benchmark/src/test/scala/com.snowplowanalytics.snowplow.analytics.scalasdk/benchmark/).

They can be run with `sbt "project benchmark" "+jmh:run -i 10 -wi 3 -f2 -t3"`.

To get details about the parameters `jmh:run -h`.

## Copyright and license

The Snowplow Scala Analytics SDK is copyright 2016-2019 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[travis-image]: https://travis-ci.org/snowplow/snowplow-scala-analytics-sdk.png?branch=master
[travis]: http://travis-ci.org/snowplow/snowplow-scala-analytics-sdk

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[release-image]: http://img.shields.io/badge/release-3.2.2-blue.svg?style=flat
[releases]: https://github.com/snowplow/snowplow-scala-analytics-sdk/releases

[scala-doc]: http://snowplow.github.io/snowplow-scala-analytics-sdk/

[enriched-events]: https://docs.snowplowanalytics.com/docs/understanding-your-pipeline/canonical-event/

[spark]: http://spark.apache.org/
[lambda]: https://aws.amazon.com/lambda/
[flink]: https://flink.apache.org/
[scalding]: https://github.com/twitter/scalding
[samza]: http://samza.apache.org/
