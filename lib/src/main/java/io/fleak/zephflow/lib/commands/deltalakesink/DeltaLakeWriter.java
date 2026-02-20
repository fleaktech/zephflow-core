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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import io.delta.kernel.*;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.expressions.Literal;
import io.delta.kernel.hook.PostCommitHook;
import io.delta.kernel.types.ArrayType;
import io.delta.kernel.types.DataType;
import io.delta.kernel.types.MapType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import io.delta.kernel.utils.CloseableIterable;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.DataFileStatus;
import io.fleak.zephflow.api.JobContext;
import io.fleak.zephflow.api.metric.FleakCounter;
import io.fleak.zephflow.api.structure.RecordFleakData;
import io.fleak.zephflow.lib.commands.databrickssink.AvroToDeltaSchemaConverter;
import io.fleak.zephflow.lib.commands.sink.AbstractBufferedFlusher;
import io.fleak.zephflow.lib.commands.sink.SimpleSinkCommand;
import io.fleak.zephflow.lib.dlq.DlqWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.jetbrains.annotations.NotNull;

@Slf4j
public class DeltaLakeWriter extends AbstractBufferedFlusher<Map<String, Object>> {

  private final DeltaLakeSinkDto.Config config;
  private final JobContext jobContext;
  private final FleakCounter sinkOutputCounter;
  private final FleakCounter outputSizeCounter;
  private final FleakCounter sinkErrorCounter;

  private Engine engine;
  private Table table;
  private StructType tableSchema;
  private boolean initialized = false;

  private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger(0);
  private final int instanceId = INSTANCE_COUNTER.incrementAndGet();
  private final ReentrantLock flushLock = new ReentrantLock();
  private ExecutorService checkpointExecutor;
  private Map<String, DataType> partitionColumnTypes;

  public DeltaLakeWriter(
      DeltaLakeSinkDto.Config config,
      JobContext jobContext,
      DlqWriter dlqWriter,
      @NonNull FleakCounter sinkOutputCounter,
      @NonNull FleakCounter outputSizeCounter,
      @NonNull FleakCounter sinkErrorCounter,
      String nodeId) {
    super(dlqWriter, jobContext, nodeId);
    this.config = config;
    this.jobContext = jobContext;
    this.sinkOutputCounter = sinkOutputCounter;
    this.outputSizeCounter = outputSizeCounter;
    this.sinkErrorCounter = sinkErrorCounter;
  }

  /** Initialize the Delta Lake writer. Must be called before using flush(). */
  public synchronized void initialize() {
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

    // Load existing table and validate it exists
    Snapshot snapshot;
    try {
      this.table = Table.forPath(engine, config.getTablePath());
      snapshot = table.getLatestSnapshot(engine);
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

    // Parse config schema for split-brain detection
    StructType configSchema = AvroToDeltaSchemaConverter.parse(config.getAvroSchema());
    log.info("Parsed config schema with {} fields", configSchema.fields().size());

    // Validate config schema against actual table schema (Split Brain detection)
    StructType physicalTableSchema = snapshot.getSchema();
    validateSchemaCompatibility(configSchema, physicalTableSchema);
    log.info("Schema validated against physical table schema");

    // Use physical schema for writes to satisfy Delta Kernel's exact equality requirement
    this.tableSchema = physicalTableSchema;

    // Safety check: Validate partition columns match between config and physical table
    List<String> tablePartitionColumns = snapshot.getPartitionColumnNames();
    List<String> configPartitionColumns =
        config.getPartitionColumns() != null ? config.getPartitionColumns() : List.of();

    if (!tablePartitionColumns.equals(configPartitionColumns)) {
      String errorMessage =
          String.format(
              "Partition column mismatch detected (Split Brain). "
                  + "Config partition columns: %s, Table partition columns: %s. "
                  + "This could cause data corruption. "
                  + "Please reconcile the configuration with the actual table schema.",
              configPartitionColumns, tablePartitionColumns);
      log.error(errorMessage);
      throw new IllegalStateException(errorMessage);
    }
    log.info(
        "Partition columns validated: {}",
        tablePartitionColumns.isEmpty() ? "(none - unpartitioned)" : tablePartitionColumns);

    // Cache partition column types for O(1) lookup during partitioning
    this.partitionColumnTypes = buildPartitionColumnTypeCache();

    // Initialize checkpoint executor before marking as initialized
    String checkpointThreadName =
        String.format(
            "DeltaLakeWriter-Checkpoint-%04X-%d",
            config.getTablePath().hashCode() & 0xFFFF, instanceId);
    this.checkpointExecutor =
        new ThreadPoolExecutor(
            1,
            1,
            0L,
            TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(1),
            r -> {
              Thread thread = new Thread(r, checkpointThreadName);
              thread.setDaemon(true);
              return thread;
            },
            (r, executor) ->
                log.warn(
                    "Checkpoint task rejected for path: {} - previous checkpoint still running. "
                        + "Table remains consistent, but checkpoints are falling behind.",
                    config.getTablePath()));

    this.initialized = true;
    log.info("Delta Lake writer initialization completed for path: {}", config.getTablePath());

    super.initialize();
  }

  // ===== TIMER CONFIGURATION =====

  @Override
  protected long getFlushIntervalMs() {
    return config.getFlushIntervalSeconds() * 1000L;
  }

  @Override
  protected String getSchedulerThreadName() {
    return String.format(
        "DeltaLakeWriter-Flush-%04X-%d", config.getTablePath().hashCode() & 0xFFFF, instanceId);
  }

  // ===== ABSTRACT METHOD IMPLEMENTATIONS =====

  @Override
  protected int getBatchSize() {
    return config.getBatchSize();
  }

  @Override
  protected SimpleSinkCommand.FlushResult doFlush(
      List<Pair<RecordFleakData, Map<String, Object>>> batch) throws Exception {
    List<Map<String, Object>> data = batch.stream().map(Pair::getRight).toList();
    return writeDataToDeltaTable(data);
  }

  @Override
  protected void ensureCanWriteRecord(Map<String, Object> record) throws Exception {
    Preconditions.checkNotNull(record, "Record is null");
    //noinspection EmptyTryBlock
    try (var ignored =
        DeltaLakeDataConverter.convertToColumnarBatch(List.of(record), tableSchema)) {
      // noop
    }
  }

  // ===== HOOK METHOD IMPLEMENTATIONS =====

  @Override
  protected void beforeFlush() {
    if (!initialized) {
      throw new IllegalStateException(
          "Delta Lake writer not initialized. Call initialize() first.");
    }
  }

  @Override
  protected void beforeWrite() {
    flushLock.lock();
  }

  @Override
  protected void afterWrite() {
    flushLock.unlock();
  }

  @Override
  protected void reportMetrics(
      SimpleSinkCommand.FlushResult result, Map<String, String> metricTags) {
    sinkOutputCounter.increase(result.successCount(), metricTags);
    outputSizeCounter.increase(result.flushedDataSize(), metricTags);
  }

  @Override
  protected void reportErrorMetrics(int errorCount, Map<String, String> metricTags) {
    sinkErrorCounter.increase(errorCount, metricTags);
  }

  // ===== DELTA LAKE WRITE LOGIC =====

  /** Write data to Delta table using Delta Kernel API */
  private SimpleSinkCommand.FlushResult writeDataToDeltaTable(List<Map<String, Object>> dataToWrite)
      throws Exception {
    log.debug("Starting Delta Lake write operation for {} records", dataToWrite.size());

    // Use cached schema from config
    log.debug("Using table schema: {}", this.tableSchema);

    // Step 2: Create transaction
    Transaction transaction = createTransaction(table);

    // Step 3: Group data by partition values if table is partitioned
    Map<Map<String, Literal>, List<Map<String, Object>>> partitionedData =
        partitionDataByColumns(dataToWrite, this.tableSchema);

    long totalDataSize = 0;
    List<CloseableIterator<Row>> dataActionIterators = new ArrayList<>();
    try {
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
    } catch (Exception e) {
      for (CloseableIterator<Row> iter : dataActionIterators) {
        try {
          iter.close();
        } catch (IOException closeEx) {
          e.addSuppressed(closeEx);
        }
      }
      log.error("Error during processing partitions", e);
      throw e;
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

        // Get the target type from cached partition column types (O(1) lookup)
        DataType targetType = partitionColumnTypes.get(partitionColumn);
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
      return Literal.ofNull(targetType);
    } else if (value instanceof String s) {
      return Literal.ofString(s);
    } else if (value instanceof Integer i) {
      return Literal.ofInt(i);
    } else if (value instanceof Long l) {
      return Literal.ofLong(l);
    } else if (value instanceof Boolean b) {
      return Literal.ofBoolean(b);
    } else if (value instanceof Float f) {
      return Literal.ofFloat(f);
    } else if (value instanceof Double d) {
      return Literal.ofDouble(d);
    } else if (value instanceof Short s) {
      return Literal.ofShort(s);
    } else if (value instanceof Byte b) {
      return Literal.ofByte(b);
    } else if (value instanceof BigDecimal bd) {
      return Literal.ofDecimal(bd, bd.precision(), bd.scale());
    } else if (value instanceof java.time.LocalDate ld) {
      return Literal.ofDate((int) ld.toEpochDay());
    } else if (value instanceof java.sql.Date d) {
      return Literal.ofDate((int) d.toLocalDate().toEpochDay());
    } else if (value instanceof java.time.Instant inst) {
      return Literal.ofTimestamp(inst.getEpochSecond() * 1_000_000 + inst.getNano() / 1000);
    } else if (value instanceof java.sql.Timestamp ts) {
      java.time.Instant inst = ts.toInstant();
      return Literal.ofTimestamp(inst.getEpochSecond() * 1_000_000 + inst.getNano() / 1000);
    } else {
      throw new IllegalArgumentException(
          String.format(
              "Unsupported data type for Literal conversion: %s. Value: %s. "
                  + "Supported types: String, Integer, Long, Boolean, Float, Double, "
                  + "Short, Byte, BigDecimal, LocalDate, Date, Instant, Timestamp.",
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

  private Map<String, DataType> buildPartitionColumnTypeCache() {
    List<String> partitionColumns = config.getPartitionColumns();
    if (partitionColumns == null || partitionColumns.isEmpty()) {
      return Map.of();
    }
    Map<String, DataType> cache = new HashMap<>();
    for (String col : partitionColumns) {
      DataType type = getColumnType(tableSchema, col);
      if (type != null) {
        cache.put(col, type);
      }
    }
    return cache;
  }

  private void createCheckpointIfReady(TransactionCommitResult commitResult) {
    List<PostCommitHook> postCommitHooks = commitResult.getPostCommitHooks();

    if (postCommitHooks == null || postCommitHooks.isEmpty()) {
      return;
    }

    List<PostCommitHook> checkpointHooks =
        postCommitHooks.stream()
            .filter(hook -> hook.getType() == PostCommitHook.PostCommitHookType.CHECKPOINT)
            .toList();

    if (checkpointHooks.isEmpty()) {
      return;
    }

    long version = commitResult.getVersion();

    // Submit checkpoint work to background thread to avoid blocking data processing
    checkpointExecutor.submit(
        () -> {
          for (PostCommitHook hook : checkpointHooks) {
            try {
              log.info(
                  "Creating checkpoint at version {} for path: {}", version, config.getTablePath());
              long startTime = System.currentTimeMillis();
              hook.threadSafeInvoke(engine);
              long duration = System.currentTimeMillis() - startTime;
              log.info(
                  "Checkpoint completed at version {} for path: {} (took {}ms)",
                  version,
                  config.getTablePath(),
                  duration);
            } catch (Exception e) {
              log.warn(
                  "Failed to create checkpoint at version {} for path: {}. "
                      + "Table is still consistent, subsequent reads may be slower.",
                  version,
                  config.getTablePath(),
                  e);
            }
          }
        });
  }

  @Override
  public void close() throws IOException {
    log.info("Closing Delta Lake writer for path: {}", config.getTablePath());

    stopFlushTimer();
    stopCheckpointExecutor();

    List<Pair<RecordFleakData, Map<String, Object>>> snapshot = swapBufferIfNotEmpty();

    if (snapshot != null) {
      log.info(
          "Flushing {} remaining buffered events before closing writer for path: {}",
          snapshot.size(),
          config.getTablePath());
      SimpleSinkCommand.FlushResult result = executeFlush(snapshot, Map.of());
      if (!result.errorOutputList().isEmpty()) {
        reportErrorMetrics(result.errorOutputList().size(), Map.of());
        if (dlqWriter == null) {
          throw new IOException(
              String.format(
                  "Failed to flush %d remaining events during close for path: %s. "
                      + "No DLQ configured, records lost.",
                  result.errorOutputList().size(), config.getTablePath()));
        }
        handleScheduledFlushErrors(result.errorOutputList());
      }
    }

    if (dlqWriter != null) {
      dlqWriter.close();
    }

    log.info("Delta Lake writer closed successfully for path: {}", config.getTablePath());
  }

  private void stopCheckpointExecutor() {
    if (checkpointExecutor == null) {
      return;
    }
    checkpointExecutor.shutdown();
    try {
      if (!checkpointExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        log.warn(
            "Checkpoint executor did not terminate in time for path: {}", config.getTablePath());
        checkpointExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      checkpointExecutor.shutdownNow();
      Thread.currentThread().interrupt();
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

    Object credential =
        credentialObjOpt.orElseThrow(
            () ->
                new IllegalStateException(
                    String.format(
                        "Credential not found for credentialId '%s' (required for %s path: %s)",
                        credentialId, storageType, tablePath)));

    applyCredentials(storageType, hadoopConf, tablePath, credential, credentialId);
  }

  private void validateSchemaCompatibility(StructType configSchema, StructType tableSchema) {
    List<String> errors = new ArrayList<>();

    log.debug(
        "Validating schema compatibility. Config fields: {}, Table fields: {}",
        configSchema.fields().size(),
        tableSchema.fields().size());

    for (StructField configField : configSchema.fields()) {
      String fieldName = configField.getName();
      int tableFieldIndex = tableSchema.indexOf(fieldName);

      if (tableFieldIndex < 0) {
        errors.add(String.format("Field '%s' exists in config but not in table", fieldName));
        continue;
      }

      StructField tableField = tableSchema.at(tableFieldIndex);

      if (!configField.getDataType().equivalent(tableField.getDataType())) {
        errors.add(
            String.format(
                "Field '%s' type mismatch: config has %s, table has %s",
                fieldName, configField.getDataType(), tableField.getDataType()));
      }

      if (!tableField.isNullable() && configField.isNullable()) {
        errors.add(
            String.format(
                "Field '%s' nullability mismatch: table is non-nullable but config allows null",
                fieldName));
      }

      // Recursively validate nested types (Structs, Arrays, Maps)
      validateNestedNullability(
          configField.getDataType(), tableField.getDataType(), fieldName, errors);
    }

    for (StructField tableField : tableSchema.fields()) {
      if (configSchema.indexOf(tableField.getName()) < 0) {
        errors.add(
            String.format("Field '%s' exists in table but not in config", tableField.getName()));
      }
    }

    if (!errors.isEmpty()) {
      String errorMessage =
          String.format(
              "Schema mismatch detected (Split Brain). "
                  + "Config schema is incompatible with physical table schema at path: %s. "
                  + "Mismatches: [%s]. "
                  + "This could cause data corruption. "
                  + "Please reconcile the configuration with the actual table schema.",
              config.getTablePath(), String.join("; ", errors));
      log.error(errorMessage);
      throw new IllegalStateException(errorMessage);
    }
  }

  private void validateNestedNullability(
      DataType configType, DataType tableType, String fieldPath, List<String> errors) {

    if (configType instanceof StructType configStruct
        && tableType instanceof StructType tableStruct) {
      for (StructField configField : configStruct.fields()) {
        int idx = tableStruct.indexOf(configField.getName());
        if (idx >= 0) {
          StructField tableField = tableStruct.at(idx);
          String nestedPath = fieldPath + "." + configField.getName();

          if (!tableField.isNullable() && configField.isNullable()) {
            errors.add(
                String.format(
                    "Field '%s' nullability mismatch: table is non-nullable but config allows null",
                    nestedPath));
          }

          validateNestedNullability(
              configField.getDataType(), tableField.getDataType(), nestedPath, errors);
        }
      }
    } else if (configType instanceof ArrayType configArray
        && tableType instanceof ArrayType tableArray) {
      if (!tableArray.containsNull() && configArray.containsNull()) {
        errors.add(
            String.format(
                "Array '%s' element nullability mismatch: table elements are non-nullable but config allows null elements",
                fieldPath));
      }
      validateNestedNullability(
          configArray.getElementType(),
          tableArray.getElementType(),
          fieldPath + "[element]",
          errors);
    } else if (configType instanceof MapType configMap && tableType instanceof MapType tableMap) {
      if (!tableMap.isValueContainsNull() && configMap.isValueContainsNull()) {
        errors.add(
            String.format(
                "Map '%s' value nullability mismatch: table values are non-nullable but config allows null values",
                fieldPath));
      }
      validateNestedNullability(
          configMap.getValueType(), tableMap.getValueType(), fieldPath + "[value]", errors);
    }
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

  @VisibleForTesting
  int testGetBufferSize() {
    return getBufferSize();
  }
}
