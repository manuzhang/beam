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

import com.google.auto.value.AutoValue;
import java.util.Base64;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.schemas.SchemaCoder;
import org.apache.beam.sdk.schemas.SchemaRegistry;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldNumber;
import org.apache.beam.sdk.schemas.annotations.SchemaIgnore;
import org.apache.beam.sdk.util.SerializableUtils;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.ImmutableList;
import org.apache.iceberg.ChangelogScanTask;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.ScanTaskParser;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

@DefaultSchema(AutoValueSchema.class)
@AutoValue
abstract class ReadTask {
  private static @MonotonicNonNull SchemaCoder<ReadTask> coder;

  static SchemaCoder<ReadTask> getCoder() {
    if (coder == null) {
      try {
        coder = SchemaRegistry.createDefault().getSchemaCoder(ReadTask.class);
      } catch (NoSuchSchemaException e) {
        throw new RuntimeException(e);
      }
    }
    return coder;
  }

  private transient @MonotonicNonNull List<FileScanTask> cachedFileScanTask;
  private transient @MonotonicNonNull List<ChangelogScanTask> cachedChangelogScanTasks;

  static Builder builder() {
    return new AutoValue_ReadTask.Builder()
        .setFileScanTaskJsons(ImmutableList.of())
        .setChangelogScanTaskBase64s(ImmutableList.of());
  }

  @SchemaFieldNumber("0")
  abstract List<String> getFileScanTaskJsons();

  @SchemaFieldNumber("1")
  abstract List<String> getChangelogScanTaskBase64s();

  @SchemaIgnore
  List<FileScanTask> getFileScanTasks() {
    if (cachedFileScanTask == null) {
      cachedFileScanTask =
          getFileScanTaskJsons().stream()
              .map(json -> ScanTaskParser.fromJson(json, true))
              .collect(Collectors.toList());
    }
    return cachedFileScanTask;
  }

  @SchemaIgnore
  List<ChangelogScanTask> getChangelogScanTasks() {
    if (cachedChangelogScanTasks == null) {
      cachedChangelogScanTasks =
          getChangelogScanTaskBase64s().stream()
              .map(
                  encodedBytes ->
                      (ChangelogScanTask)
                          SerializableUtils.deserializeFromByteArray(
                              Base64.getDecoder().decode(encodedBytes), "ChangelogScanTask"))
              .collect(Collectors.toList());
    }
    return cachedChangelogScanTasks;
  }

  @SchemaIgnore
  boolean isChangelogTask() {
    return !getChangelogScanTaskBase64s().isEmpty();
  }

  @SchemaIgnore
  long getTaskCount() {
    return isChangelogTask() ? getChangelogScanTaskBase64s().size() : getFileScanTaskJsons().size();
  }

  @SchemaIgnore
  long getSize(long from, long to) {
    if (isChangelogTask()) {
      return getChangelogScanTasks().subList((int) from, (int) to).stream()
          .mapToLong(ChangelogScanTask::sizeBytes)
          .sum();
    } else {
      return getFileScanTasks().subList((int) from, (int) to).stream()
          .mapToLong(FileScanTask::length)
          .sum();
    }
  }

  @AutoValue.Builder
  abstract static class Builder {
    abstract Builder setFileScanTaskJsons(List<String> jsons);

    @SchemaIgnore
    Builder setCombinedScanTask(CombinedScanTask combinedScanTask) {
      List<String> fileScanTaskJsons =
          combinedScanTask.tasks().stream()
              .map(ScanTaskParser::toJson)
              .collect(Collectors.toList());
      return setFileScanTaskJsons(fileScanTaskJsons);
    }

    @SchemaIgnore
    Builder setChangelogScanTask(ChangelogScanTask changelogScanTask) {
      return setChangelogScanTaskBase64s(
          ImmutableList.of(
              Base64.getEncoder()
                  .encodeToString(SerializableUtils.serializeToByteArray(changelogScanTask))));
    }

    abstract Builder setChangelogScanTaskBase64s(List<String> encodedBytes);

    abstract ReadTask build();
  }
}
