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

package org.apache.paimon.rest;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Database;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.catalog.Prediction;
import org.apache.paimon.catalog.PredictionOutput;
import org.apache.paimon.catalog.PredictionSource;
import org.apache.paimon.catalog.PredictionSpec;
import org.apache.paimon.catalog.PredictionStatus;
import org.apache.paimon.catalog.TableMetadata;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.rest.requests.SubmitPredictionRequest;
import org.apache.paimon.rest.responses.ErrorResponse;
import org.apache.paimon.rest.responses.GetPredictionResponse;
import org.apache.paimon.rest.responses.ListPredictionsResponse;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.sink.BatchTableCommit;
import org.apache.paimon.table.sink.BatchTableWrite;
import org.apache.paimon.table.sink.BatchWriteBuilder;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.utils.InternalRowUtils;

import org.apache.paimon.shade.jackson2.com.fasterxml.jackson.core.JsonProcessingException;

import okhttp3.mockwebserver.MockResponse;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.paimon.rest.RESTApi.MAX_RESULTS;
import static org.apache.paimon.rest.RESTApi.PAGE_TOKEN;
import static org.apache.paimon.rest.RESTCatalogServerUtils.parseRequest;

/** Executable prediction endpoints for the local REST catalog reference integration. */
final class RESTCatalogPredictionHandler implements AutoCloseable {

    private static final String DATABASE = "database";
    private static final String TABLE = "table";
    private static final String NAME = "name";
    private static final String STATE = "state";

    private final RESTCatalogServer server;
    private final RESTFileSystemCatalog catalog;
    private final RESTCatalogModelHandler modelHandler;
    private final Map<String, Database> databaseStore;
    private final Map<String, TableMetadata> tableMetadataStore;
    private final String predictionsPath;
    private final Map<String, PredictionJob> jobs = new ConcurrentHashMap<>();
    private final Map<String, PredictionJob> jobsByIdempotencyToken = new ConcurrentHashMap<>();
    private final AtomicLong nextId = new AtomicLong();
    private final ExecutorService executor;

    private volatile CountDownLatch executionGate = new CountDownLatch(0);
    private volatile ExecutionGate outputCreatedGate = ExecutionGate.open();
    private volatile ExecutionGate outputCommittedGate = ExecutionGate.open();
    private volatile @Nullable CyclicBarrier submissionBarrier;

    RESTCatalogPredictionHandler(
            RESTCatalogServer server,
            RESTFileSystemCatalog catalog,
            RESTCatalogModelHandler modelHandler,
            Map<String, Database> databaseStore,
            Map<String, TableMetadata> tableMetadataStore,
            String predictionsPath) {
        this.server = server;
        this.catalog = catalog;
        this.modelHandler = modelHandler;
        this.databaseStore = databaseStore;
        this.tableMetadataStore = tableMetadataStore;
        this.predictionsPath = predictionsPath;
        this.executor =
                Executors.newSingleThreadExecutor(
                        runnable -> {
                            Thread thread =
                                    new Thread(runnable, "paimon-reference-prediction-executor");
                            thread.setDaemon(true);
                            return thread;
                        });
    }

    MockResponse handle(
            String method, String resourcePath, String data, Map<String, String> parameters)
            throws Exception {
        if (predictionsPath.equals(resourcePath)) {
            if ("POST".equals(method)) {
                return submit(data);
            } else if ("GET".equals(method)) {
                return list(parameters);
            }
            return new MockResponse().setResponseCode(404);
        }

        String suffix = resourcePath.substring((predictionsPath + "/").length());
        String[] segments = suffix.split("/");
        String predictionId = RESTUtil.decodeString(segments[0]);
        if (segments.length == 1 && "GET".equals(method)) {
            return get(predictionId);
        }
        if (segments.length == 2 && "cancel".equals(segments[1]) && "POST".equals(method)) {
            return cancel(predictionId);
        }
        return new MockResponse().setResponseCode(404);
    }

    void pauseExecution() {
        executionGate = new CountDownLatch(1);
    }

    void resumeExecution() {
        executionGate.countDown();
    }

    void synchronizeNextSubmissions(int parties) {
        submissionBarrier = new CyclicBarrier(parties);
    }

    void pauseAfterOutputCreated() {
        outputCreatedGate = ExecutionGate.paused();
    }

    void awaitOutputCreated() throws InterruptedException {
        outputCreatedGate.awaitReached();
    }

    void resumeAfterOutputCreated() {
        outputCreatedGate.resume();
    }

    void pauseAfterOutputCommitted() {
        outputCommittedGate = ExecutionGate.paused();
    }

    void awaitOutputCommitted() throws InterruptedException {
        outputCommittedGate.awaitReached();
    }

    void resumeAfterOutputCommitted() {
        outputCommittedGate.resume();
    }

    private MockResponse submit(String data) throws Exception {
        SubmitPredictionRequest request = parseRequest(data, SubmitPredictionRequest.class);
        PredictionJob existing = jobsByIdempotencyToken.get(request.getIdempotencyToken());
        if (existing != null) {
            return idempotencyResponse(existing, data);
        }

        Identifier source = request.getSource().toIdentifier();
        PredictionSpec spec = request.toPredictionSpec();
        MockResponse validationError = validate(source, spec);
        if (validationError != null) {
            return validationError;
        }

        FileStoreTable sourceTable = (FileStoreTable) catalog.getTable(source);
        long snapshotId = resolveSnapshot(sourceTable, spec);
        String predictionId = String.format("prediction-%020d", nextId.incrementAndGet());
        PredictionJob job =
                new PredictionJob(
                        predictionId, request.getSource(), spec, snapshotId, data, executionGate);
        awaitSubmissionBarrier();
        PredictionJob selected =
                jobsByIdempotencyToken.compute(
                        request.getIdempotencyToken(),
                        (token, current) -> {
                            if (current != null) {
                                return current;
                            }
                            jobs.put(predictionId, job);
                            try {
                                executor.submit(() -> execute(job));
                                return job;
                            } catch (RuntimeException e) {
                                jobs.remove(predictionId, job);
                                throw e;
                            }
                        });
        return selected == job
                ? response(new GetPredictionResponse(job.snapshot()), 202)
                : idempotencyResponse(selected, data);
    }

    private MockResponse idempotencyResponse(PredictionJob existing, String data) {
        if (existing.requestPayload.equals(data)) {
            return response(new GetPredictionResponse(existing.snapshot()), 202);
        }
        return error(
                ErrorResponse.RESOURCE_TYPE_PREDICTION,
                existing.predictionId,
                "idempotencyToken is already used by another prediction",
                409);
    }

    private void awaitSubmissionBarrier() throws Exception {
        CyclicBarrier barrier = submissionBarrier;
        if (barrier != null) {
            barrier.await(10, TimeUnit.SECONDS);
            submissionBarrier = null;
        }
    }

    @Nullable
    private MockResponse validate(Identifier source, PredictionSpec spec) {
        if (!databaseStore.containsKey(source.getDatabaseName())) {
            return error(
                    ErrorResponse.RESOURCE_TYPE_DATABASE,
                    source.getDatabaseName(),
                    "Database does not exist",
                    404);
        }
        if (!tableMetadataStore.containsKey(source.getFullName())) {
            return error(
                    ErrorResponse.RESOURCE_TYPE_TABLE,
                    source.getObjectName(),
                    "Table does not exist",
                    404);
        }
        Identifier model = spec.model();
        if (!databaseStore.containsKey(model.getDatabaseName())) {
            return error(
                    ErrorResponse.RESOURCE_TYPE_DATABASE,
                    model.getDatabaseName(),
                    "Database does not exist",
                    404);
        }
        if (modelHandler.getModel(model) == null) {
            return error(
                    ErrorResponse.RESOURCE_TYPE_MODEL,
                    model.getObjectName(),
                    "Model does not exist",
                    404);
        }
        if (spec.where() != null) {
            return error(null, null, "The reference provider does not evaluate where", 501);
        }

        TableSchema schema;
        try {
            schema = ((FileStoreTable) catalog.getTable(source)).schema();
        } catch (Exception e) {
            return error(
                    ErrorResponse.RESOURCE_TYPE_TABLE,
                    source.getObjectName(),
                    "Table does not exist",
                    404);
        }
        List<String> fields = schema.fieldNames();
        List<String> unsupportedScanOptions =
                unsupportedScanOptions(schema.options(), spec.options());
        if (!unsupportedScanOptions.isEmpty()) {
            return error(
                    null,
                    null,
                    "Prediction does not support scan options: " + unsupportedScanOptions,
                    400);
        }
        for (String input : spec.inputs().values()) {
            if (!fields.contains(input)) {
                return error(
                        ErrorResponse.RESOURCE_TYPE_COLUMN,
                        input,
                        "Prediction input column does not exist",
                        404);
            }
        }
        for (String key : mergeKeyColumns(schema, spec)) {
            if (!fields.contains(key)) {
                return error(
                        ErrorResponse.RESOURCE_TYPE_COLUMN,
                        key,
                        "Prediction merge key column does not exist",
                        404);
            }
            if ("response".equals(key) || "error".equals(key)) {
                return error(null, null, "mergeKeyColumns use a reserved output name", 400);
            }
        }
        return null;
    }

    private long resolveSnapshot(FileStoreTable table, PredictionSpec spec) {
        CoreOptions options = new CoreOptions(spec.options());
        Long snapshotId = options.scanSnapshotId();
        if (snapshotId != null) {
            Snapshot snapshot = table.snapshotManager().snapshot(snapshotId);
            if (snapshot == null) {
                throw new IllegalArgumentException("Snapshot " + snapshotId + " does not exist");
            }
            return snapshot.id();
        }
        String tagName = options.scanTagName();
        if (tagName != null) {
            return table.tagManager().getOrThrow(tagName).id();
        }
        Snapshot latest = table.snapshotManager().latestSnapshot();
        if (latest == null) {
            throw new IllegalArgumentException("The prediction source has no snapshot");
        }
        return latest.id();
    }

    private void execute(PredictionJob job) {
        @Nullable Identifier preparedOutput = null;
        try {
            if (!job.markRunning()) {
                job.completeCanceled();
                return;
            }
            job.executionGate.await();
            checkCanceled(job);
            preparedOutput = executePrediction(job);
            Identifier output = preparedOutput;
            if (!job.publishSuccess(output, () -> publishOutput(output))) {
                cleanupOutput(output);
                job.completeCanceled();
            }
        } catch (InterruptedException e) {
            cleanupOutput(job.stagingOutput());
            job.completeCanceled();
            Thread.currentThread().interrupt();
        } catch (Throwable t) {
            cleanupOutput(preparedOutput == null ? job.stagingOutput() : preparedOutput);
            if (job.isTerminating()) {
                job.completeCanceled();
            } else {
                job.completeFailure(
                        t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage());
            }
        }
    }

    private Identifier executePrediction(PredictionJob job) throws Exception {
        Identifier sourceIdentifier = job.source.toIdentifier();
        FileStoreTable source = (FileStoreTable) catalog.getTable(sourceIdentifier);
        Map<String, String> scanOptions = new HashMap<>(job.spec.options());
        scanOptions.remove(CoreOptions.SCAN_TAG_NAME.key());
        scanOptions.put(CoreOptions.SCAN_SNAPSHOT_ID.key(), Long.toString(job.snapshotId));
        source = source.copy(scanOptions);

        Identifier outputIdentifier =
                Identifier.create(
                        sourceIdentifier.getDatabaseName(),
                        sourceIdentifier.getObjectName()
                                + "__"
                                + job.predictionId.replace('-', '_'));
        List<String> mergeKeyColumns = mergeKeyColumns(source.schema(), job.spec);
        Schema outputSchema = outputSchema(source.schema(), mergeKeyColumns);
        catalog.createTable(outputIdentifier, outputSchema, false);
        job.setStagingOutput(outputIdentifier);
        FileStoreTable output = (FileStoreTable) catalog.getTable(outputIdentifier);
        outputCreatedGate.reachAndAwait();
        checkCanceled(job);

        Map<String, Integer> fieldIndexes = fieldIndexes(source.schema());
        BatchWriteBuilder writeBuilder = output.newBatchWriteBuilder();
        ReadBuilder readBuilder = source.newReadBuilder();
        try (RecordReaderIterator<InternalRow> rows =
                        new RecordReaderIterator<>(
                                readBuilder
                                        .newRead()
                                        .createReader(readBuilder.newScan().plan().splits()));
                BatchTableWrite write = writeBuilder.newWrite();
                BatchTableCommit commit = writeBuilder.newCommit()) {
            while (rows.hasNext()) {
                checkCanceled(job);
                InternalRow sourceRow = rows.next();
                write.write(
                        toOutputRow(
                                sourceRow,
                                source.schema(),
                                fieldIndexes,
                                mergeKeyColumns,
                                job.spec));
            }
            checkCanceled(job);
            commit.commit(write.prepareCommit());
        }

        outputCommittedGate.reachAndAwait();
        checkCanceled(job);
        return outputIdentifier;
    }

    private void publishOutput(Identifier identifier) {
        try {
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
            Snapshot snapshot = table.snapshotManager().latestSnapshot();
            registerOutputTable(identifier, table);
            server.setTableSnapshot(
                    identifier,
                    snapshot,
                    snapshot.totalRecordCount(),
                    0L,
                    0L,
                    snapshot.timeMillis());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void cleanupOutput(@Nullable Identifier identifier) {
        if (identifier == null) {
            return;
        }
        tableMetadataStore.remove(identifier.getFullName());
        try {
            catalog.dropTable(identifier, true);
        } catch (Exception ignored) {
            // Best-effort cleanup for a test reference provider.
        }
    }

    private void registerOutputTable(Identifier identifier, FileStoreTable table) {
        TableSchema schema = table.schema();
        Map<String, String> options = new HashMap<>(schema.options());
        options.put(CoreOptions.PATH.key(), catalog.getTableLocation(identifier).toString());
        tableMetadataStore.put(
                identifier.getFullName(),
                new TableMetadata(schema.copy(options), false, UUID.randomUUID().toString()));
    }

    private static Schema outputSchema(TableSchema source, List<String> mergeKeyColumns) {
        Schema.Builder builder =
                Schema.newBuilder().option(CoreOptions.SNAPSHOT_IGNORE_EMPTY_COMMIT.key(), "false");
        Map<String, DataField> fields =
                source.fields().stream().collect(Collectors.toMap(DataField::name, field -> field));
        for (String name : mergeKeyColumns) {
            builder.column(name, fields.get(name).type());
        }
        return builder.column("response", DataTypes.STRING())
                .column("error", DataTypes.STRING())
                .build();
    }

    private static List<String> unsupportedScanOptions(
            Map<String, String> tableOptions, Map<String, String> requestOptions) {
        List<String> unsupported = new ArrayList<>();
        collectUnsupportedScanOptions(tableOptions, unsupported);
        collectUnsupportedScanOptions(requestOptions, unsupported);
        return unsupported;
    }

    private static void collectUnsupportedScanOptions(
            Map<String, String> options, List<String> unsupported) {
        for (String key : options.keySet()) {
            if ((key.startsWith("scan.") || key.startsWith("incremental-"))
                    && !CoreOptions.SCAN_SNAPSHOT_ID.key().equals(key)
                    && !CoreOptions.SCAN_TAG_NAME.key().equals(key)
                    && !unsupported.contains(key)) {
                unsupported.add(key);
            }
        }
    }

    private static List<String> mergeKeyColumns(TableSchema source, PredictionSpec spec) {
        return spec.mergeKeyColumns().isEmpty() ? source.primaryKeys() : spec.mergeKeyColumns();
    }

    private static Map<String, Integer> fieldIndexes(TableSchema schema) {
        Map<String, Integer> indexes = new HashMap<>();
        List<String> fields = schema.fieldNames();
        for (int i = 0; i < fields.size(); i++) {
            indexes.put(fields.get(i), i);
        }
        return indexes;
    }

    private static InternalRow toOutputRow(
            InternalRow sourceRow,
            TableSchema sourceSchema,
            Map<String, Integer> fieldIndexes,
            List<String> mergeKeyColumns,
            PredictionSpec spec)
            throws JsonProcessingException {
        Object[] values = new Object[mergeKeyColumns.size() + 2];
        int outputIndex = 0;
        for (String key : mergeKeyColumns) {
            int sourceIndex = fieldIndexes.get(key);
            DataType type = sourceSchema.fields().get(sourceIndex).type();
            values[outputIndex++] =
                    InternalRowUtils.copy(InternalRowUtils.get(sourceRow, sourceIndex, type), type);
        }

        Map<String, Object> response = new LinkedHashMap<>();
        for (Map.Entry<String, String> input : spec.inputs().entrySet()) {
            int sourceIndex = fieldIndexes.get(input.getValue());
            DataType type = sourceSchema.fields().get(sourceIndex).type();
            Object value = InternalRowUtils.get(sourceRow, sourceIndex, type);
            response.put(input.getKey(), value == null ? null : value.toString());
        }
        values[outputIndex] = BinaryString.fromString(RESTApi.toJson(response));
        values[outputIndex + 1] = null;
        return GenericRow.of(values);
    }

    private MockResponse get(String predictionId) {
        PredictionJob job = jobs.get(predictionId);
        return job == null
                ? notFound(predictionId)
                : response(new GetPredictionResponse(job.snapshot()), 200);
    }

    private MockResponse cancel(String predictionId) {
        PredictionJob job = jobs.get(predictionId);
        if (job == null) {
            return notFound(predictionId);
        }
        job.cancel();
        return response(new GetPredictionResponse(job.snapshot()), 200);
    }

    private MockResponse list(Map<String, String> parameters) {
        List<PredictionJob> matching =
                jobs.values().stream()
                        .filter(job -> matches(job, parameters))
                        .sorted(Comparator.comparing(PredictionJob::id).reversed())
                        .collect(Collectors.toList());
        String pageToken = parameters.get(PAGE_TOKEN);
        if (pageToken != null) {
            matching =
                    matching.stream()
                            .filter(job -> job.id().compareTo(pageToken) < 0)
                            .collect(Collectors.toList());
        }
        int maxResults = maxResults(parameters);
        boolean hasMore = matching.size() > maxResults;
        List<PredictionJob> page = hasMore ? matching.subList(0, maxResults) : matching;
        List<GetPredictionResponse> responses =
                page.stream()
                        .map(job -> new GetPredictionResponse(job.snapshot()))
                        .collect(Collectors.toList());
        String nextPageToken = hasMore ? page.get(page.size() - 1).id() : null;
        return response(new ListPredictionsResponse(responses, nextPageToken), 200);
    }

    private static boolean matches(PredictionJob job, Map<String, String> parameters) {
        Prediction prediction = job.snapshot();
        return matches(parameters.get(DATABASE), prediction.source().database())
                && matches(parameters.get(TABLE), prediction.source().table())
                && matches(parameters.get(NAME), prediction.name())
                && matches(parameters.get(STATE), prediction.status().state().name());
    }

    private static boolean matches(@Nullable String expected, @Nullable String actual) {
        return expected == null || expected.equals(actual);
    }

    private static int maxResults(Map<String, String> parameters) {
        String value = parameters.get(MAX_RESULTS);
        if (value == null) {
            return RESTCatalogServer.DEFAULT_MAX_RESULTS;
        }
        int requested = Integer.parseInt(value);
        return requested <= 0
                ? RESTCatalogServer.DEFAULT_MAX_RESULTS
                : Math.min(requested, RESTCatalogServer.DEFAULT_MAX_RESULTS);
    }

    private static void checkCanceled(PredictionJob job) throws InterruptedException {
        if (job.isTerminating() || Thread.currentThread().isInterrupted()) {
            throw new InterruptedException("Prediction was canceled");
        }
    }

    private static MockResponse notFound(String predictionId) {
        return error(
                ErrorResponse.RESOURCE_TYPE_PREDICTION,
                predictionId,
                "Prediction does not exist",
                404);
    }

    private static MockResponse error(
            @Nullable String resourceType,
            @Nullable String resourceName,
            String message,
            int status) {
        return response(new ErrorResponse(resourceType, resourceName, message, status), status);
    }

    private static MockResponse response(Object response, int status) {
        try {
            return new MockResponse()
                    .setResponseCode(status)
                    .setBody(RESTApi.toJson(response))
                    .addHeader("Content-Type", "application/json");
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        executionGate.countDown();
        outputCreatedGate.resume();
        outputCommittedGate.resume();
        executor.shutdownNow();
        try {
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static final class ExecutionGate {

        private final CountDownLatch reached;
        private final CountDownLatch resume;

        private ExecutionGate(int count) {
            this.reached = new CountDownLatch(count);
            this.resume = new CountDownLatch(count);
        }

        private static ExecutionGate open() {
            return new ExecutionGate(0);
        }

        private static ExecutionGate paused() {
            return new ExecutionGate(1);
        }

        private void reachAndAwait() throws InterruptedException {
            reached.countDown();
            resume.await();
        }

        private void awaitReached() throws InterruptedException {
            if (!reached.await(10, TimeUnit.SECONDS)) {
                throw new AssertionError("Prediction execution did not reach the expected phase");
            }
        }

        private void resume() {
            resume.countDown();
        }
    }

    private static final class PredictionJob {

        private final String predictionId;
        private final PredictionSource source;
        private final PredictionSpec spec;
        private final long snapshotId;
        private final String requestPayload;
        private final CountDownLatch executionGate;

        private PredictionStatus status =
                new PredictionStatus(PredictionStatus.State.PENDING, null);
        private @Nullable PredictionOutput output;
        private @Nullable Long startTime;
        private @Nullable Long endTime;
        private @Nullable Identifier stagingOutput;

        private PredictionJob(
                String predictionId,
                PredictionSource source,
                PredictionSpec spec,
                long snapshotId,
                String requestPayload,
                CountDownLatch executionGate) {
            this.predictionId = predictionId;
            this.source = source;
            this.spec = spec;
            this.snapshotId = snapshotId;
            this.requestPayload = requestPayload;
            this.executionGate = executionGate;
        }

        private synchronized String id() {
            return predictionId;
        }

        private synchronized boolean markRunning() {
            if (status.state() != PredictionStatus.State.PENDING) {
                return false;
            }
            status = new PredictionStatus(PredictionStatus.State.RUNNING, null);
            startTime = System.currentTimeMillis();
            return true;
        }

        private synchronized boolean publishSuccess(
                Identifier outputIdentifier, Runnable publisher) {
            if (status.state() != PredictionStatus.State.RUNNING) {
                return false;
            }
            publisher.run();
            output = new PredictionOutput(new PredictionSource(outputIdentifier));
            status =
                    new PredictionStatus(
                            PredictionStatus.State.TERMINATED,
                            new PredictionStatus.TerminationDetails(
                                    PredictionStatus.TerminationCode.SUCCESS, null));
            endTime = System.currentTimeMillis();
            return true;
        }

        private synchronized void completeFailure(String message) {
            if (status.isTerminal()) {
                return;
            }
            status =
                    new PredictionStatus(
                            PredictionStatus.State.TERMINATED,
                            new PredictionStatus.TerminationDetails(
                                    PredictionStatus.TerminationCode.FAILED, message));
            endTime = System.currentTimeMillis();
        }

        private synchronized void completeCanceled() {
            if (status.isTerminal()) {
                return;
            }
            status =
                    new PredictionStatus(
                            PredictionStatus.State.TERMINATED,
                            new PredictionStatus.TerminationDetails(
                                    PredictionStatus.TerminationCode.CANCELED, null));
            endTime = System.currentTimeMillis();
        }

        private synchronized void setStagingOutput(Identifier stagingOutput) {
            this.stagingOutput = stagingOutput;
        }

        private synchronized @Nullable Identifier stagingOutput() {
            return stagingOutput;
        }

        private synchronized boolean isTerminating() {
            return status.state() == PredictionStatus.State.TERMINATING;
        }

        private synchronized void cancel() {
            if (status.isTerminal() || status.state() == PredictionStatus.State.TERMINATING) {
                return;
            }
            if (status.state() == PredictionStatus.State.PENDING) {
                completeCanceled();
            } else {
                status = new PredictionStatus(PredictionStatus.State.TERMINATING, null);
            }
        }

        private synchronized Prediction snapshot() {
            return new Prediction(
                    predictionId,
                    source,
                    spec.name(),
                    status,
                    snapshotId,
                    spec.model(),
                    output,
                    startTime,
                    endTime);
        }
    }
}
