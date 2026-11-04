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
package org.apache.gluten.extension

import org.apache.gluten.execution.DeltaScanTransformer
import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.offload.OffloadSingleNode

import org.apache.spark.sql.delta.DeltaParquetFileFormat
import org.apache.spark.sql.delta.files.TahoeBatchFileIndex
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{DataType, StructType}

case class OffloadDeltaScan(enableNativeDmlRowIndexScan: Boolean) extends OffloadSingleNode {
  private val DeletionVectorsUseMetadataRowIndexKey =
    "spark.databricks.delta.deletionVectors.useMetadataRowIndex"

  // Row-index columns used by Delta DML with either row-index generation path.
  private val generatedRowIndexColumnNames =
    Set(
      DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME,
      ParquetFileFormat.ROW_INDEX_TEMPORARY_COLUMN_NAME)
  private val generatedDeletionVectorMetadataColumnNames =
    Set(
      DeltaParquetFileFormat.ROW_INDEX_COLUMN_NAME,
      DeltaParquetFileFormat.IS_ROW_DELETED_COLUMN_NAME)
  // ParquetFileFormat.ROW_INDEX, the generated field Delta adds to the file metadata struct in
  // PreprocessTableWithDVs when deletionVectors.useMetadataRowIndex is on. Only meaningful nested
  // under _metadata -- a user column may legitimately be called row_index.
  private val metadataRowIndexFieldName = "row_index"
  // TahoeBatchFileIndex.actionType as set by Delta's DELETE, UPDATE and MERGE commands.
  private val dmlActionTypes = Set("delete", "update", "merge")

  override def offload(plan: SparkPlan): SparkPlan = plan match {
    case scan: FileSourceScanExec if isDeltaLogScan(scan) =>
      FallbackTags.add(scan, "fallback Delta _delta_log scan")
      scan
    case scan: FileSourceScanExec if shouldFallbackDeletionVectorDmlScan(scan) =>
      FallbackTags.add(scan, "fallback Delta DV DML row-index scan by configuration")
      scan
    case scan: FileSourceScanExec
        if DeltaScanUtils.isDeltaScan(scan) &&
          shouldFallbackGeneratedDeletionVectorMetadataScan(scan) =>
      FallbackTags.add(scan, "fallback Delta scan requiring generated DV metadata")
      scan
    case scan: FileSourceScanExec if DeltaScanUtils.isDeltaScan(scan) =>
      DeltaScanTransformer(scan)
    case other => other
  }

  /**
   * The scoped escape hatch: with the config off, the DELETE/UPDATE/MERGE target scan that produces
   * file paths and row indexes for deletion-vector writes stays on Spark, while every other scan
   * keeps offloading. The whole check lives here, on the scan alone: Delta builds every DML target
   * relation over a [[TahoeBatchFileIndex]] carrying the command name, which survives AQE stage
   * splits and arbitrary join placement, and only DV-writing DML reads a row-index column from that
   * relation; DML that rewrites whole files does not, and remains eligible for native execution.
   */
  private def shouldFallbackDeletionVectorDmlScan(scan: FileSourceScanExec): Boolean = {
    !enableNativeDmlRowIndexScan && isDmlTargetScan(scan) && scanReadsRowIndexColumn(scan)
  }

  private def isDmlTargetScan(scan: FileSourceScanExec): Boolean = {
    scan.relation.location match {
      case index: TahoeBatchFileIndex => dmlActionTypes.contains(index.actionType)
      case _ => false
    }
  }

  private def isRowIndexColumn(name: String, dataType: DataType): Boolean = {
    generatedRowIndexColumnNames.contains(name) ||
    (name == FileFormat.METADATA_NAME && (dataType match {
      case struct: StructType => struct.fieldNames.contains(metadataRowIndexFieldName)
      case _ => false
    }))
  }

  private def scanReadsGeneratedDeletionVectorMetadataColumn(
      scan: FileSourceScanExec): Boolean = {
    scanReadsColumn(
      scan,
      (name, _) => generatedDeletionVectorMetadataColumnNames.contains(name))
  }

  private def scanReadsRowIndexColumn(scan: FileSourceScanExec): Boolean = {
    scanReadsColumn(scan, isRowIndexColumn)
  }

  private def scanReadsColumn(
      scan: FileSourceScanExec,
      predicate: (String, DataType) => Boolean): Boolean = {
    val outputFields = scan.output.iterator.map(attribute => (attribute.name, attribute.dataType))
    val requiredFields =
      scan.requiredSchema.fields.iterator.map(field => (field.name, field.dataType))
    (outputFields ++ requiredFields).exists {
      case (name, dataType) => predicate(name, dataType)
    }
  }

  private def isDeltaLogScan(scan: FileSourceScanExec): Boolean = {
    scan.relation.location.rootPaths.exists {
      path =>
        val root = path.toString
        root.contains("/_delta_log") || root.contains("\\_delta_log") || root.endsWith("_delta_log")
    }
  }

  private def shouldFallbackGeneratedDeletionVectorMetadataScan(
      scan: FileSourceScanExec): Boolean = {
    // Without metadata row indexes, Delta's JVM reader generates the internal DV columns.
    // Native scans bypass that synthesis; Spark's own metadata row index remains supported.
    val useMetadataRowIndex =
      scan.relation.sparkSession.sessionState.conf
        .getConfString(DeletionVectorsUseMetadataRowIndexKey, "true")
        .toBoolean
    !useMetadataRowIndex && scanReadsGeneratedDeletionVectorMetadataColumn(scan)
  }
}
