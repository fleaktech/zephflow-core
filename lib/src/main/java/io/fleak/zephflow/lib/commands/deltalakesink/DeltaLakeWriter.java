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
package io.fleak.zephflow.lib.commands.deltalakesink;

import static io.fleak.zephflow.lib.commands.deltalakesink.DeltaLakeStorageCredentialUtils.applyCredentials;
import static io.fleak.zephflow.lib.commands.deltalakesink.DeltaLakeStorageCredentialUtils.resolveStorageType;
import static io.fleak.zephflow.lib.utils.MiscUtils.*;
import static java.util.stream.Collectors.toList;

import io.delta.kernel.*;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.fleak.zephflow.api.ErrorOutput;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import java.io.IOException;
import java.util.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class DeltaLakeWriter implements SimpleSinkCommand.Flusher<Map<String, Object>> {

  private final DeltaLakeSinkDto.Config config;
  private final JobContext jobContext;
  private Engine engine;
  private Table table;
  private boolean initialized = false;

  private final List<Pair<RecordFleakData, Map<String, Object>>> buffer = new ArrayList<>();
  private final Object bufferLock = new Object();

  public DeltaLakeWriter(DeltaLakeSinkDto.Config config, JobContext jobContext) {
    this.config = config;
    this.jobContext = jobContext;
  }

  /** Initialize the Delta Lake writer. Must be called before using flush(). */
  public void initialize() {
    if (initialized) {
      log.warn("Delta Lake writer already initialized for path: {}", config.getTablePath());
      return;
    }

    log.info("Initializing Delta Lake writer for path: {}", config.getTablePath());

    // Initialize Hadoop configuration
    Configuration hadoopConf = new Configuration();

    // Add user-specified Hadoop configuration first
    if (config.getHadoopConfiguration() != null) {
      for (Map.Entry<String, String> entry : config.getHadoopConfiguration().entrySet()) {
        hadoopConf.set(entry.getKey(), entry.getValue());
      }
    }

    // Handle credentials from credentialId if provided
    if (config.getCredentialId() != null) {
      applyCredentialsForStorage(hadoopConf, config.getTablePath());
    }

    this.engine = DefaultEngine.create(hadoopConf);

    // Load existing table - fail if table doesn't exist (as per requirement)
    try {
      this.table = Table.forPath(engine, config.getTablePath());
      log.info("Loaded existing Delta table at path: {}", config.getTablePath());
    } catch (Exception e) {
      String errorMessage =
          String.format(
              "Delta table does not exist at path: %s. "
                  + "This sink requires pre-existing tables and does not support automatic table creation. "
                  + "Please create the Delta table first before using this sink.",
              config.getTablePath());
      log.error(errorMessage, e);
      throw new IllegalStateException(errorMessage, e);
    }

    this.initialized = true;
    log.info("Delta Lake writer initialization completed for path: {}", config.getTablePath());
  }

  @Override
  public SimpleSinkCommand.FlushResult flush(
      SimpleSinkCommand.PreparedInputEvents<Map<String, Object>> preparedInputEvents)
      throws Exception {

    if (!initialized) {
      throw new IllegalStateException(
          "Delta Lake writer not initialized. Call initialize() first.");
    }

    if (preparedInputEvents.preparedList().isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    List<Pair<RecordFleakData, Map<String, Object>>> incomingEventPairs =
        preparedInputEvents.rawAndPreparedList();
    int batchSize = config.getBatchSize();

    synchronized (bufferLock) {
      buffer.addAll(incomingEventPairs);
      log.debug(
          "Accumulated {} events in buffer. Total buffer size: {} / {}",
          incomingEventPairs.size(),
          buffer.size(),
          batchSize);

      if (buffer.size() >= batchSize) {
        log.info(
            "Buffer reached batch size ({}). Flushing {} events to Delta table.",
            batchSize,
            buffer.size());
        return flushBuffer();
      }

      log.debug(
          "Buffer not full yet ({} / {}). Waiting for more events before flushing to Delta.",
          buffer.size(),
          batchSize);
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }
  }

  private SimpleSinkCommand.FlushResult flushBuffer() throws Exception {
    if (buffer.isEmpty()) {
      return new SimpleSinkCommand.FlushResult(0, 0, List.of());
    }

    List<Pair<RecordFleakData, Map<String, Object>>> bufferedEventPairs = new ArrayList<>(buffer);
    buffer.clear();

    // Extract just the prepared maps for writing
    List<Map<String, Object>> dataToWrite =
        bufferedEventPairs.stream().map(Pair::getRight).collect(toList());

    log.info(
        "Flushing buffer with {} events to Delta table at path: {}",
        dataToWrite.size(),
        config.getTablePath());

    try {
      return writeDataToDeltaTable(dataToWrite);
    } catch (Exception e) {
      log.error("Error writing to Delta table at path: {}", config.getTablePath(), e);
      List<ErrorOutput> errorOutputs =
          bufferedEventPairs.stream()
              .map(
                  pair ->
                      new ErrorOutput(
                          pair.getLeft(), "Failed to write to Delta table: " + e.getMessage()))
              .toList();
      return new SimpleSinkCommand.FlushResult(0, 0, errorOutputs);
    }
  }

  private SimpleSinkCommand.FlushResult writeDataToDeltaTable(List<Map<String, Object>> dataToWrite)
      throws Exception {
    log.debug("Starting Delta Lake write operation for {} records", dataToWrite.size());

    // Table existence already validated during initialization - use the existing table
    StructType tableSchema = table.getLatestSnapshot(engine).getSchema();
    log.debug("Using table schema: {}", tableSchema);

    // Step 2: Create transaction
    Transaction transaction = createTransaction(table);

    // Step 3: Group data by partition values if table is partitioned
    Map<Map<String, Literal>, List<Map<String, Object>>> partitionedData =
        partitionDataByColumns(dataToWrite, tableSchema);

    long totalDataSize = 0;

    try {
      List<CloseableIterator<Row>> dataActionIterators = new ArrayList<>();

      // Step 5: Process each partition separately
      for (Map.Entry<Map<String, Literal>, List<Map<String, Object>>> partition :
          partitionedData.entrySet()) {
        Map<String, Literal> partitionValues = partition.getKey();
        List<Map<String, Object>> partitionData = partition.getValue();

        PartitionResult result =
            processPartition(transaction, partitionData, partitionValues, tableSchema);
        totalDataSize += result.dataSize();
        dataActionIterators.add(result.dataActions());
      }

      // Step 6: Combine all data action iterators into a single stream and commit
      log.info("Committing transaction with data for {} records", dataToWrite.size());

      try (CloseableIterator<Row> allActionsIterator =
          new CombinedCloseableIterator<>(dataActionIterators)) {
        TransactionCommitResult commitResult =
            transaction.commit(
                engine,
                new CloseableIterable<>() {
                  @Override
                  public @NotNull CloseableIterator<Row> iterator() {
                    return allActionsIterator;
                  }

                  @Override
                  public void close() {
                    // Iterator is closed by try-with-resources
                  }
                });

        log.info(
            "Delta Lake write operation completed successfully for {} records, committed as version {}",
            dataToWrite.size(),
            commitResult.getVersion());

        if (config.isEnableAutoCheckpoint()) {
          createCheckpointIfReady(commitResult);
        }

        return new SimpleSinkCommand.FlushResult(dataToWrite.size(), totalDataSize, List.of());
      }

    } catch (Exception e) {
      log.error("Error during Delta Lake write operation", e);
      throw e;
    }
  }

  private Transaction createTransaction(Table targetTable) {
    log.debug("Creating Delta Lake transaction");

    TransactionBuilder builder =
        targetTable.createTransactionBuilder(
            engine,
            "ZephFlow Delta Lake Sink", // application name
            Operation.WRITE);
    return builder.build(engine);
  }

  private Map<Map<String, Literal>, List<Map<String, Object>>> partitionDataByColumns(
      List<Map<String, Object>> dataToWrite, StructType tableSchema) {

    List<String> partitionColumns = config.getPartitionColumns();

    if (partitionColumns == null || partitionColumns.isEmpty()) {
      // Unpartitioned table - all data goes to single partition with empty partition values
      Map<String, Literal> emptyPartitionValues = Map.of();
      return Map.of(emptyPartitionValues, dataToWrite);
    }

    // Group data by partition column values
    Map<Map<String, Literal>, List<Map<String, Object>>> partitionedData = new HashMap<>();

    for (Map<String, Object> record : dataToWrite) {
      Map<String, Literal> partitionValues = new HashMap<>();

      // Extract partition values from this record
      for (String partitionColumn : partitionColumns) {
        Object value = record.get(partitionColumn);

        // Get the target type from table schema for proper null handling
        DataType targetType = getColumnType(tableSchema, partitionColumn);
        if (targetType == null) {
          List<String> availableColumns =
              tableSchema.fields().stream().map(StructField::getName).collect(toList());
          throw new IllegalArgumentException(
              "Partition column '"
                  + partitionColumn
                  + "' not found in table schema. "
                  + "Available columns: "
                  + String.join(", ", availableColumns));
        }

        Literal partitionValue = convertToLiteral(value, targetType);
        partitionValues.put(partitionColumn, partitionValue);
      }

      // Add record to the appropriate partition group
      partitionedData.computeIfAbsent(partitionValues, k -> new ArrayList<>()).add(record);
    }

    log.debug(
        "Data partitioned into {} partitions based on columns: {}",
        partitionedData.size(),
        partitionColumns);

    return partitionedData;
  }

  private Literal convertToLiteral(Object value, DataType targetType) {
    if (value == null) {
      // Use the actual target type for null, not arbitrary STRING assumption
      return Literal.ofNull(targetType);
    } else if (value instanceof String) {
      return Literal.ofString((String) value);
    } else if (value instanceof Integer) {
      return Literal.ofInt((Integer) value);
    } else if (value instanceof Long) {
      return Literal.ofLong((Long) value);
    } else if (value instanceof Boolean) {
      return Literal.ofBoolean((Boolean) value);
    } else {
      // Fail fast for unknown types - don't hide bugs with toString()
      throw new IllegalArgumentException(
          String.format(
              "Unsupported data type for Literal conversion: %s. Value: %s. "
                  + "Add explicit handling for this type instead of using toString() fallback.",
              value.getClass().getName(), value));
    }
  }

  /** Get the data type for a column from the table schema */
  private DataType getColumnType(StructType schema, String columnName) {
    for (StructField field : schema.fields()) {
      if (field.getName().equals(columnName)) {
        return field.getDataType();
      }
    }
    return null; // Column not found
  }

  private void createCheckpointIfReady(TransactionCommitResult commitResult) {
    List<PostCommitHook> postCommitHooks = commitResult.getPostCommitHooks();

    if (postCommitHooks == null || postCommitHooks.isEmpty()) {
      log.debug(
          "No post-commit hooks to execute at version {} for path: {}",
          commitResult.getVersion(),
          config.getTablePath());
      return;
    }

    List<PostCommitHook> checkpointHooks =
        postCommitHooks.stream()
            .filter(hook -> hook.getType() == PostCommitHook.PostCommitHookType.CHECKPOINT)
            .toList();

    if (checkpointHooks.isEmpty()) {
      log.debug(
          "Table not ready for checkpoint at version {} for path: {}",
          commitResult.getVersion(),
          config.getTablePath());
      return;
    }

    for (PostCommitHook hook : checkpointHooks) {
      try {
        long version = commitResult.getVersion();
        log.info(
            "Table is ready for checkpoint at version {}. Creating checkpoint for path: {}",
            version,
            config.getTablePath());

        long startTime = System.currentTimeMillis();
        hook.threadSafeInvoke(engine);
        long duration = System.currentTimeMillis() - startTime;

        log.info(
            "Successfully created checkpoint at version {} for path: {} (took {}ms)",
            version,
            config.getTablePath(),
            duration);
      } catch (Exception e) {
        log.warn(
            "Failed to create checkpoint at version {} for path: {}. "
                + "This is not critical - the table is still consistent, but subsequent reads may be slower.",
            commitResult.getVersion(),
            config.getTablePath(),
            e);
      }
    }
  }

  @Override
  public void close() throws IOException {
    log.info("Closing Delta Lake writer for path: {}", config.getTablePath());

    synchronized (bufferLock) {
      if (!buffer.isEmpty()) {
        log.info(
            "Flushing {} remaining buffered events before closing writer for path: {}",
            buffer.size(),
            config.getTablePath());
        try {
          flushBuffer();
        } catch (Exception e) {
          log.error("Error flushing buffer during close for path: {}", config.getTablePath(), e);
          throw new IOException("Failed to flush remaining events during close", e);
        }
      }
    }
  }

  /**
   * Apply credentials to Hadoop configuration based on storage path and auto-detect credential
   * type. Supports S3 (UsernamePasswordCredential), Azure (ApiKeyCredential), GCS (GcpCredential).
   * For HDFS, configure authentication directly via hadoopConfiguration.
   */
  private void applyCredentialsForStorage(Configuration hadoopConf, String tablePath) {
    String credentialId = config.getCredentialId();

    DeltaLakeStorageCredentialUtils.StorageType storageType = resolveStorageType(tablePath);

    Optional<?> credentialObjOpt =
        switch (storageType) {
          case S3 -> lookupUsernamePasswordCredentialOpt(jobContext, credentialId);
          case GCS -> lookupGcpCredentialOpt(jobContext, credentialId);
          case ABS -> lookupApiKeyCredentialOpt(jobContext, credentialId);
          default -> Optional.of(new Object()); // dummy object
        };

    if (credentialObjOpt.isEmpty()) {
      log.warn(
          "{} path requires credential for credentialId: {}, but nothing is found",
          tablePath,
          credentialId);
    }

    applyCredentials(
        storageType, hadoopConf, tablePath, credentialObjOpt.orElseThrow(), credentialId);
  }

  /** Result of processing a single partition */
  private record PartitionResult(long dataSize, CloseableIterator<Row> dataActions) {}

  /**
   * Process a single partition by converting data to columnar format, writing Parquet files, and
   * generating append actions. Extracted method to avoid arrow anti-pattern.
   */
  private PartitionResult processPartition(
      Transaction transaction,
      List<Map<String, Object>> partitionData,
      Map<String, Literal> partitionValues,
      StructType tableSchema)
      throws Exception {

    log.debug(
        "Processing partition with {} records, partition values: {}",
        partitionData.size(),
        partitionValues);

    // Convert data to columnar format
    try (CloseableIterator<FilteredColumnarBatch> columnarData =
        DeltaLakeDataConverter.convertToColumnarBatch(partitionData, tableSchema)) {

      // Get transaction state
      Row txnState = transaction.getTransactionState(engine);

      // Transform logical data to physical data
      try (CloseableIterator<FilteredColumnarBatch> physicalData =
          Transaction.transformLogicalData(engine, txnState, columnarData, partitionValues)) {

        // Get write context for this partition
        DataWriteContext writeContext =
            Transaction.getWriteContext(engine, txnState, partitionValues);

        // Write physical data to Parquet files using the engine
        try (CloseableIterator<DataFileStatus> dataFiles =
            engine
                .getParquetHandler()
                .writeParquetFiles(
                    writeContext.getTargetDirectory(),
                    physicalData,
                    writeContext.getStatisticsColumns())) {

          // Calculate data size and collect file statuses
          long partitionDataSize = 0;
          List<DataFileStatus> fileStatusList = new ArrayList<>();
          while (dataFiles.hasNext()) {
            DataFileStatus fileStatus = dataFiles.next();
            fileStatusList.add(fileStatus);
            partitionDataSize += fileStatus.getSize();
          }

          // Generate append actions from the written data files - create simple iterator from list
          CloseableIterator<Row> dataActions =
              Transaction.generateAppendActions(
                  engine,
                  txnState,
                  new CloseableIterator<>() {
                    private final Iterator<DataFileStatus> iter = fileStatusList.iterator();

                    @Override
                    public boolean hasNext() {
                      return iter.hasNext();
                    }

                    @Override
                    public DataFileStatus next() {
                      return iter.next();
                    }

                    @Override
                    public void close() {
                      /* Nothing to close for list iterator */
                    }
                  },
                  writeContext);

          return new PartitionResult(partitionDataSize, dataActions);
        }
      }
    }
  }

  /** Iterator that combines multiple CloseableIterators without hiding resource cleanup errors */
  private static class CombinedCloseableIterator<T> implements CloseableIterator<T> {
    private final List<CloseableIterator<T>> iterators;
    private int currentIndex = 0;

    public CombinedCloseableIterator(List<CloseableIterator<T>> iterators) {
      this.iterators = iterators;
    }

    @Override
    public boolean hasNext() {
      while (currentIndex < iterators.size()) {
        if (iterators.get(currentIndex).hasNext()) {
          return true;
        }
        currentIndex++;
      }
      return false;
    }

    @Override
    public T next() {
      while (currentIndex < iterators.size()) {
        CloseableIterator<T> current = iterators.get(currentIndex);
        if (current.hasNext()) {
          return current.next();
        }
        currentIndex++;
      }
      throw new NoSuchElementException();
    }

    @Override
    public void close() throws IOException {
      IOException firstException = null;

      for (CloseableIterator<T> iterator : iterators) {
        try {
          iterator.close();
        } catch (IOException e) {
          if (firstException == null) {
            firstException = e;
          } else {
            firstException.addSuppressed(e);
          }
        }
      }

      if (firstException != null) {
        throw firstException; // Don't hide cleanup failures
      }
    }
  }
}
