/**
 * Copyright 2025 Fleak Tech Inc.
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.fleak.zephflow.lib.commands.sink;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

/**
 * Strategy interface for writing records to files for blob storage upload. Different
 * implementations handle different file formats (text-based like JSON/CSV/JSONL, or binary like
 * Parquet).
 *
 * @param <T> The type of records to write
 */
public interface BlobFileWriter<T> {

  /**
   * Writes records to temporary file(s) for upload. May return multiple files for large datasets
   * (e.g., Parquet with row group splits).
   *
   * @param records The records to write
   * @param tempDir The directory to create the temp file(s) in
   * @return List of temp files containing the serialized records (never empty if records non-empty)
   * @throws Exception if writing fails
   */
  List<File> writeToTempFiles(List<T> records, Path tempDir) throws Exception;

  /**
   * Returns the file extension for the output file format.
   *
   * @return The file extension (e.g., "json", "parquet")
   */
  String getFileExtension();

  /**
   * Validates if a record can be written in this format.
   *
   * @param record The record to validate
   * @throws Exception if the record cannot be written
   */
  void validateRecord(T record) throws Exception;
}
