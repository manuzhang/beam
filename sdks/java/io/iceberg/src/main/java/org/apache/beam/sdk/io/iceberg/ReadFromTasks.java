/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.iceberg;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.beam.sdk.io.range.OffsetRange;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.Row;
import org.apache.iceberg.AddedRowsScanTask;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.ContentScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.DeletedDataFileScanTask;
import org.apache.iceberg.DeletedRowsScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericDeleteFilter;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.io.CloseableIterable;

/**
 * Bounded read implementation.
 *
 * <p>For each {@link ReadTask}, reads Iceberg {@link Record}s, and converts to Beam {@link Row}s.
 *
 * <p>Implemented as an SDF to leverage communicating bundle size (i.e. {@link DoFn.GetSize}) to the
 * runner, to help with scaling decisions.
 */
@DoFn.BoundedPerElement
class ReadFromTasks extends DoFn<KV<ReadTaskDescriptor, ReadTask>, Row> {
  private final IcebergScanConfig scanConfig;
  private final Counter scanTasksCompleted =
      Metrics.counter(ReadFromTasks.class, "scanTasksCompleted");

  ReadFromTasks(IcebergScanConfig scanConfig) {
    this.scanConfig = scanConfig;
  }

  @Setup
  public void setup() {
    TableCache.setup(scanConfig);
  }

  @ProcessElement
  public void process(
      @Element KV<ReadTaskDescriptor, ReadTask> element,
      RestrictionTracker<OffsetRange, Long> tracker,
      OutputReceiver<Row> out)
      throws IOException, ExecutionException, InterruptedException {
    ReadTask readTask = element.getValue();
    Table table = TableCache.get(scanConfig.getTableIdentifier());
    Schema beamSchema = IcebergUtils.icebergSchemaToBeamSchema(scanConfig.getProjectedSchema());
    Schema outputSchema = IcebergUtils.icebergSchemaToBeamSchema(scanConfig.getOutputSchema());

    for (long l = tracker.currentRestriction().getFrom();
        l < tracker.currentRestriction().getTo();
        l++) {
      if (!tracker.tryClaim(l)) {
        return;
      }

      if (readTask.isChangelogTask()) {
        ChangelogScanTask task = readTask.getChangelogScanTasks().get((int) l);
        try (CloseableIterable<Record> reader = readChangelogRecords(table, task)) {
          outputRecords(reader, task, beamSchema, outputSchema, out);
        }
      } else {
        FileScanTask task = readTask.getFileScanTasks().get((int) l);
        try (CloseableIterable<Record> fullIterable =
            ReadUtils.createReader(task, table, scanConfig.getRequiredSchema())) {
          CloseableIterable<Record> reader = ReadUtils.maybeApplyFilter(fullIterable, scanConfig);

          for (Record record : reader) {
            Row row = IcebergUtils.icebergRecordToBeamRow(beamSchema, record);
            out.output(row);
          }
        }
      }
      scanTasksCompleted.inc();
    }
  }

  private CloseableIterable<Record> readChangelogRecords(Table table, ChangelogScanTask task)
      throws IOException {
    if (task instanceof AddedRowsScanTask) {
      AddedRowsScanTask addedRowsScanTask = (AddedRowsScanTask) task;
      return readDataFileTask(
          table,
          asDataFileTask(addedRowsScanTask),
          addedRowsScanTask.deletes(),
          scanConfig.getRequiredSchema());
    } else if (task instanceof DeletedDataFileScanTask) {
      DeletedDataFileScanTask deletedDataFileScanTask = (DeletedDataFileScanTask) task;
      return readDataFileTask(
          table,
          asDataFileTask(deletedDataFileScanTask),
          deletedDataFileScanTask.existingDeletes(),
          scanConfig.getRequiredSchema());
    } else if (task instanceof DeletedRowsScanTask) {
      return readDeletedRows(table, (DeletedRowsScanTask) task);
    } else {
      throw new UnsupportedOperationException("Unsupported changelog task: " + task.getClass());
    }
  }

  private CloseableIterable<Record> readDataFileTask(
      Table table,
      ContentScanTask<DataFile> task,
      List<DeleteFile> deletes,
      org.apache.iceberg.Schema expectedSchema)
      throws IOException {
    FileScanTask fileScanTask = new ChangelogFileScanTask(task, deletes, table.schema());
    GenericDeleteFilter deleteFilter =
        new GenericDeleteFilter(table.io(), fileScanTask, table.schema(), expectedSchema);
    CloseableIterable<Record> fullIterable =
        ReadUtils.createReader(fileScanTask, table, deleteFilter.requiredSchema());
    CloseableIterable<Record> deletedRowsFiltered = deleteFilter.filter(fullIterable);
    return ReadUtils.maybeApplyFilter(deletedRowsFiltered, scanConfig);
  }

  private CloseableIterable<Record> readDeletedRows(Table table, DeletedRowsScanTask task)
      throws IOException {
    ContentScanTask<DataFile> dataFileTask = asDataFileTask(task);
    List<DeleteFile> allDeletes = new ArrayList<>(task.existingDeletes());
    allDeletes.addAll(task.addedDeletes());

    FileScanTask allDeletesTask =
        new ChangelogFileScanTask(dataFileTask, allDeletes, table.schema());
    GenericDeleteFilter allDeletesFilter =
        new GenericDeleteFilter(
            table.io(), allDeletesTask, table.schema(), scanConfig.getRequiredSchema());
    org.apache.iceberg.Schema readSchema = allDeletesFilter.requiredSchema();

    GenericDeleteFilter existingDeletesFilter =
        new GenericDeleteFilter(
            table.io(),
            new ChangelogFileScanTask(dataFileTask, task.existingDeletes(), table.schema()),
            table.schema(),
            readSchema);
    GenericDeleteFilter addedDeletesFilter =
        new GenericDeleteFilter(
            table.io(),
            new ChangelogFileScanTask(dataFileTask, task.addedDeletes(), table.schema()),
            table.schema(),
            readSchema);

    CloseableIterable<Record> fullIterable =
        ReadUtils.createReader(allDeletesTask, table, readSchema);
    CloseableIterable<Record> withoutExistingDeletes = existingDeletesFilter.filter(fullIterable);
    CloseableIterable<Record> deletedRows =
        CloseableIterable.filter(
            withoutExistingDeletes, record -> isDeletedBy(record, addedDeletesFilter));
    return ReadUtils.maybeApplyFilter(deletedRows, scanConfig);
  }

  private boolean isDeletedBy(Record record, GenericDeleteFilter deleteFilter) {
    if (deleteFilter.hasPosDeletes()) {
      Long position = (Long) record.getField(MetadataColumns.ROW_POSITION.name());
      if (position != null && deleteFilter.deletedRowPositions().isDeleted(position)) {
        return true;
      }
    }

    return deleteFilter.hasEqDeletes() && deleteFilter.eqDeletedRowFilter().test(record);
  }

  private void outputRecords(
      CloseableIterable<Record> records,
      ChangelogScanTask task,
      Schema beamSchema,
      Schema outputSchema,
      OutputReceiver<Row> out) {
    for (Record record : records) {
      Row row = IcebergUtils.icebergRecordToBeamRow(beamSchema, record);
      if (scanConfig.getIncludeChangelogMetadata()) {
        row =
            Row.withSchema(outputSchema)
                .addValues(row.getValues())
                .addValues(task.operation().name(), task.changeOrdinal(), task.commitSnapshotId())
                .build();
      }
      out.output(row);
    }
  }

  @SuppressWarnings("unchecked")
  private static ContentScanTask<DataFile> asDataFileTask(ChangelogScanTask task) {
    return (ContentScanTask<DataFile>) task;
  }

  @GetSize
  public double getSize(
      @Element KV<ReadTaskDescriptor, ReadTask> element, @Restriction OffsetRange restriction) {
    // TODO(ahmedabu98): this is actually the file byte size, likely compressed.
    //  find a way to output the actual Beam Row byte size.
    return element.getValue().getSize(restriction.getFrom(), restriction.getTo());
  }

  @GetInitialRestriction
  public OffsetRange getInitialRange(@Element KV<ReadTaskDescriptor, ReadTask> element) {
    return new OffsetRange(0, element.getValue().getTaskCount());
  }

  private static class ChangelogFileScanTask implements FileScanTask {
    private final ContentScanTask<DataFile> task;
    private final List<DeleteFile> deletes;
    private final org.apache.iceberg.Schema schema;

    ChangelogFileScanTask(
        ContentScanTask<DataFile> task,
        List<DeleteFile> deletes,
        org.apache.iceberg.Schema schema) {
      this.task = task;
      this.deletes = Collections.unmodifiableList(new ArrayList<>(deletes));
      this.schema = schema;
    }

    @Override
    public DataFile file() {
      return task.file();
    }

    @Override
    public List<DeleteFile> deletes() {
      return deletes;
    }

    @Override
    public org.apache.iceberg.Schema schema() {
      return schema;
    }

    @Override
    public PartitionSpec spec() {
      return task.spec();
    }

    @Override
    public StructLike partition() {
      return task.partition();
    }

    @Override
    public long start() {
      return task.start();
    }

    @Override
    public long length() {
      return task.length();
    }

    @Override
    public Expression residual() {
      return task.residual();
    }

    @Override
    public Iterable<FileScanTask> split(long splitSize) {
      throw new UnsupportedOperationException("Changelog file scan task splitting is unsupported");
    }
  }
}
