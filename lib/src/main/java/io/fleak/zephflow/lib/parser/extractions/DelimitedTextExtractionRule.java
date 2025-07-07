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
package io.fleak.zephflow.lib.parser.extractions;

import com.google.common.annotations.VisibleForTesting;
import io.fleak.zephflow.api.structure.FleakData;
import io.fleak.zephflow.api.structure.RecordFleakData;
import java.io.IOException;
import java.io.StringReader;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.NonNull;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;

/**
 * Parses a single line of delimited text using Apache Commons CSV.
 *
 * <p>This implementation correctly handles quoted values and special characters, providing a more
 * robust alternative to String.split().
 *
 * @author bolei (original)
 * @author Gemini (refactored)
 */
public class DelimitedTextExtractionRule implements ExtractionRule {
  private final String[] columns;
  private final CSVFormat csvFormat;

  /**
   * Creates a DelimitedTextExtractionRule from a separator and a comma-separated string of column
   * names.
   *
   * <p>This factory method handles cases where column names are quoted and may contain commas. For
   * example, a valid columnsStr would be: {@code "id,\"last, first\",email,status"}.
   *
   * @param separator The delimiter character for the data rows that the final rule will parse.
   * @param columnsStr A single, comma-separated string defining the column headers.
   * @return A configured instance of {@link DelimitedTextExtractionRule}.
   * @throws IllegalArgumentException if the columnsStr is malformed and cannot be parsed.
   */
  public static DelimitedTextExtractionRule createDelimitedTextExtractionRule(
      @NonNull String separator, @NonNull String columnsStr) {
    List<String> columns = parseColumnNames(columnsStr);
    return new DelimitedTextExtractionRule(separator, columns);
  }

  @VisibleForTesting
  static List<String> parseColumnNames(@NonNull String columnsStr) {
    if (StringUtils.isBlank(columnsStr)) {
      return List.of();
    }

    List<String> columns;
    // Use a CSVParser to correctly handle quoted column names.
    // The format is default (comma-separated).
    try (CSVParser parser = new CSVParser(new StringReader(columnsStr), CSVFormat.DEFAULT)) {
      CSVRecord record =
          parser.getRecords().stream()
              .findFirst()
              .orElseThrow(
                  () ->
                      new IOException("Parser returned no records for a non-empty column string."));

      // Convert the parsed record into a list of strings.
      columns = StreamSupport.stream(record.spliterator(), false).collect(Collectors.toList());

    } catch (IOException e) {
      // If parsing fails, it indicates a malformed input string.
      throw new IllegalArgumentException(
          "Failed to parse the provided columnsStr: " + columnsStr, e);
    }
    return columns;
  }

  /**
   * Constructs the rule with a specified separator and column headers.
   *
   * @param separator The character used to separate values. Must be a single character string.
   * @param columns The list of column names corresponding to the values.
   */
  private DelimitedTextExtractionRule(@NonNull String separator, @NonNull List<String> columns) {
    if (separator.length() != 1) {
      throw new IllegalArgumentException("Separator must be a single character.");
    }
    this.columns = columns.toArray(new String[0]);

    // Define the CSV format once to reuse it
    this.csvFormat =
        CSVFormat.DEFAULT
            .builder()
            .setDelimiter(separator)
            .setHeader(this.columns)
            .setSkipHeaderRecord(
                false) // We are parsing a single data line, not a file with a header
            .build();
  }

  /**
   * Extracts data from a single raw string record.
   *
   * @param raw The raw string line to parse.
   * @return A RecordFleakData object containing the parsed key-value pairs.
   * @throws IOException if a parsing error occurs.
   * @throws IllegalArgumentException if the number of parsed values exceeds the number of columns.
   */
  @Override
  public RecordFleakData extract(String raw) throws IOException {
    if (StringUtils.isEmpty(raw)) {
      return new RecordFleakData();
    }

    // Use try-with-resources for the parser, which is good practice
    try (CSVParser parser = new CSVParser(new StringReader(raw), this.csvFormat)) {
      CSVRecord record =
          parser.getRecords().stream()
              .findFirst()
              .orElseThrow(
                  () -> new IOException("Failed to parse the raw data into a CSV record."));

      if (record.size() > this.columns.length) {
        throw new IllegalArgumentException(
            "More values are provided ("
                + record.size()
                + ") than columns defined ("
                + this.columns.length
                + ").");
      }

      // record.toMap() conveniently creates a map from column headers to values
      Map<String, String> payload = record.toMap();
      return (RecordFleakData) FleakData.wrap(payload);
    }
  }
}
