/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iceberg.spark.procedures;

import java.util.Iterator;
import org.apache.iceberg.actions.ConvertEqualityDeleteFiles;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.procedures.BoundProcedure;
import org.apache.spark.sql.connector.catalog.procedures.ProcedureParameter;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * A procedure that converts equality delete files to position delete files.
 *
 * <p>This is useful for improving read performance as position deletes are more efficient to apply
 * than equality deletes.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * CALL catalog.system.convert_equality_deletes('db.table')
 * }</pre>
 *
 * @see org.apache.iceberg.spark.actions.SparkActions#convertEqualityDeletes(org.apache.iceberg.Table)
 */
class ConvertEqualityDeleteFilesProcedure extends BaseProcedure {

  static final String NAME = "convert_equality_deletes";

  private static final ProcedureParameter TABLE_PARAM =
      requiredInParameter("table", DataTypes.StringType);

  private static final ProcedureParameter[] PARAMETERS = new ProcedureParameter[] {TABLE_PARAM};

  private static final StructType OUTPUT_TYPE =
      new StructType(
          new StructField[] {
            new StructField(
                "rewritten_delete_files_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(
                "added_delete_files_count", DataTypes.IntegerType, false, Metadata.empty()),
            new StructField(
                "rewritten_delete_records_count", DataTypes.LongType, false, Metadata.empty()),
            new StructField(
                "added_delete_records_count", DataTypes.LongType, false, Metadata.empty())
          });

  public static SparkProcedures.ProcedureBuilder builder() {
    return new Builder<ConvertEqualityDeleteFilesProcedure>() {
      @Override
      protected ConvertEqualityDeleteFilesProcedure doBuild() {
        return new ConvertEqualityDeleteFilesProcedure(tableCatalog());
      }
    };
  }

  private ConvertEqualityDeleteFilesProcedure(TableCatalog tableCatalog) {
    super(tableCatalog);
  }

  @Override
  public BoundProcedure bind(StructType inputType) {
    return this;
  }

  @Override
  public ProcedureParameter[] parameters() {
    return PARAMETERS;
  }

  @Override
  public Iterator<Scan> call(InternalRow args) {
    ProcedureInput input = new ProcedureInput(spark(), tableCatalog(), PARAMETERS, args);
    Identifier tableIdent = input.ident(TABLE_PARAM);

    return modifyIcebergTable(
        tableIdent,
        table -> {
          ConvertEqualityDeleteFiles action = actions().convertEqualityDeletes(table);
          ConvertEqualityDeleteFiles.Result result = action.execute();
          return asScanIterator(OUTPUT_TYPE, toOutputRow(result));
        });
  }

  private InternalRow toOutputRow(ConvertEqualityDeleteFiles.Result result) {
    return newInternalRow(
        result.convertedEqualityDeleteFilesCount(),
        result.addedPositionDeleteFilesCount(),
        result.rewrittenDeleteRecordsCount(),
        result.addedDeleteRecordsCount());
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String description() {
    return "ConvertEqualityDeleteFilesProcedure";
  }
}
