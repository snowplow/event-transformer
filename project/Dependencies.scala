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
import sbt._

object Dependencies {

  object V {
    val igluCore       = "1.1.3"
    val cats           = "2.9.0"
    val circe          = "0.14.3"
    // Scala (test only)
    val specs2         = "4.19.0"
    val scalaCheck     = "1.17.0"
  }

  val igluCore         = "com.snowplowanalytics"      %% "iglu-core-circe"      % V.igluCore
  val cats             = "org.typelevel"              %% "cats-core"            % V.cats
  val circeParser      = "io.circe"                   %% "circe-parser"         % V.circe
  val circeGeneric     = "io.circe"                   %% "circe-generic"        % V.circe
  // Scala (test only)
  val specs2           = "org.specs2"                 %% "specs2-core"          % V.specs2     % Test
  val specs2Scalacheck = "org.specs2"                 %% "specs2-scalacheck"    % V.specs2     % Test
  val scalacheck       = "org.scalacheck"             %% "scalacheck"           % V.scalaCheck % Test
  val circeLiteral     = "io.circe"                   %% "circe-literal"        % V.circe      % Test
}
