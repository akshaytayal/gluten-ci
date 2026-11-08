/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution.datasources

import org.apache.gluten.config.GlutenConfig

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.GlutenSQLTestsBaseTrait

class GlutenParquetV1AggregatePushDownSuite
  extends ParquetV1AggregatePushDownSuite
  with GlutenSQLTestsBaseTrait {}

class GlutenParquetV2AggregatePushDownSuite
  extends ParquetV2AggregatePushDownSuite
  with GlutenSQLTestsBaseTrait {

  // The native Parquet writer ignores `parquet.column.statistics.enabled` and always writes column
  // statistics, so the file is written by Spark's writer. The aggregate push-down scan falls back.
  testGluten("SPARK-57746: aggregate push-down fails when Parquet statistics are missing") {
    Seq("false", "true").foreach {
      enableVectorizedReader =>
        withTempPath {
          dir =>
            val path = dir.getCanonicalPath
            withSQLConf(GlutenConfig.NATIVE_WRITER_ENABLED.key -> "false") {
              spark
                .range(10)
                .selectExpr("CAST(id AS INT) AS i")
                .coalesce(1)
                .write
                .option("parquet.column.statistics.enabled", "false")
                .parquet(path)
            }
            withTempView("t") {
              spark.read.parquet(path).createOrReplaceTempView("t")
              withSQLConf(
                aggPushDownEnabledKey -> "true",
                vectorizedReaderEnabledKey -> enableVectorizedReader) {
                checkErrorMatchPVals(
                  exception = interceptAggPushDownError("SELECT min(i) FROM t"),
                  condition = "PARQUET_AGGREGATE_PUSH_DOWN_UNSUPPORTED.NO_MIN_MAX",
                  parameters = Map("filePath" -> ".*", "config" -> aggPushDownEnabledKey)
                )
                checkErrorMatchPVals(
                  exception = interceptAggPushDownError("SELECT count(i) FROM t"),
                  condition = "PARQUET_AGGREGATE_PUSH_DOWN_UNSUPPORTED.NO_NUM_NULLS",
                  parameters = Map("filePath" -> ".*", "config" -> aggPushDownEnabledKey)
                )
              }
            }
        }
    }
  }

  private def interceptAggPushDownError(query: String): SparkUnsupportedOperationException = {
    val e = intercept[Exception](spark.sql(query).collect())
    var cause: Throwable = e
    while (cause != null && !cause.isInstanceOf[SparkUnsupportedOperationException]) {
      cause = cause.getCause
    }
    assert(cause != null, s"Expected a SparkUnsupportedOperationException but got: $e")
    cause.asInstanceOf[SparkUnsupportedOperationException]
  }
}

class GlutenOrcV1AggregatePushDownSuite
  extends OrcV1AggregatePushDownSuite
  with GlutenSQLTestsBaseTrait {}

class GlutenOrcV2AggregatePushDownSuite
  extends OrcV2AggregatePushDownSuite
  with GlutenSQLTestsBaseTrait {}
