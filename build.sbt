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

lazy val root = project
  .in(file("."))
  .settings(
    Seq[Setting[_]](
      name := "snowplow-scala-analytics-sdk",
      organization := "com.snowplowanalytics",
      description := "Scala analytics SDK for Snowplow",
      scalaVersion := "2.13.10",
      crossScalaVersions := Seq("2.12.17", "2.13.10", "3.2.1")
    )
  )
  .enablePlugins(SiteScaladocPlugin)
  .enablePlugins(PreprocessPlugin)
  .settings(BuildSettings.dynVerSettings)
  .settings(BuildSettings.buildSettings)
  .settings(BuildSettings.publishSettings)
  .settings(BuildSettings.mimaSettings)
  .settings(BuildSettings.scoverageSettings)
  .settings(BuildSettings.sbtSiteSettings)
  .settings(BuildSettings.formattingSettings)
  .settings(
    Seq(
      shellPrompt := { _ => name.value + " > " }
    )
  )
  .settings(
    libraryDependencies ++= Seq(
      // Scala
      Dependencies.igluCore,
      Dependencies.cats,
      Dependencies.circeParser,
      Dependencies.circeGeneric,
      // Scala (test only)
      Dependencies.specs2,
      Dependencies.specs2Scalacheck,
      Dependencies.scalacheck,
      Dependencies.circeLiteral
    )
  )

lazy val benchmark = project
  .in(file("benchmark"))
  .dependsOn(root % "test->test")
  .enablePlugins(JmhPlugin)
