package com.sincera.npam.service;

import com.google.cloud.bigquery.*;
import com.google.cloud.storage.*;
import com.sincera.npam.dto.MetricEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.TimeUnit;

@Service
public class BigQueryService {

    private final BigQuery bigQuery;
    private final Storage storage;
    private final String project;
    private final String dataset;
    private final String table;
    private final TableId tableId;
    private final String dlqBucket;

    private final int maxAttempts = 4;
    private final long initialBackoffMs = 200L;

    public BigQueryService(BigQuery bigQuery,
                           Storage storage,
                           @Value("${app.bigquery.project-id}") String project,
                           @Value("${app.bigquery.dataset}") String dataset,
                           @Value("${app.bigquery.table}") String table,
                           @Value("${app.bigquery.dlq-bucket:}") String dlqBucket) {
        this.bigQuery = bigQuery;
        this.storage = storage;
        this.project = project;
        this.dataset = dataset;
        this.table = table;
        this.tableId = TableId.of(project, dataset, table);
        this.dlqBucket = dlqBucket;
    }

    public boolean writeBatch(List<MetricEvent> events, LookupService lookupService) {
        List<InsertAllRequest.RowToInsert> rows = new ArrayList<>(events.size());
        for (MetricEvent e : events) {
            Map<String, Object> row = new HashMap<>();
            // map fields to table columns
            String ts = e.getTimestamp();
            long epoch = toEpochSec(ts);
            row.put("event_timestamp", ts);
            row.put("event_epoch", epoch);
            row.put("collection_interval", "60m");
            row.put("device_name", e.getDevice_name());
            row.put("device_type", e.getDevice_type());
            row.put("device_ip_address", null);
            row.put("interface_name", e.getInterface_name());
            String metricType = lookupService.normalize(e.getParameter_name());
            row.put("metric_type", metricType);
            row.put("raw_value", e.getRaw_value() == null ? 0L : e.getRaw_value());
            row.put("value", e.getValue());
            row.put("source_metric_type", e.getParameter_name());
            row.put("source_system", e.getSourceSystem() == null ? "Maestro" : e.getSourceSystem());

            String insertId = generateInsertId(e);
            rows.add(InsertAllRequest.RowToInsert.of(insertId, row));
        }

        List<InsertAllRequest.RowToInsert> toTry = rows;
        int attempt = 0;
        long backoff = initialBackoffMs;
        while (attempt < maxAttempts && !toTry.isEmpty()) {
            InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);
            toTry.forEach(builder::addRow);
            InsertAllRequest request = builder.build();
            InsertAllResponse response = bigQuery.insertAll(request);
            if (!response.hasErrors()) return true;

            // collect failed rows
            Map<Long, List<BigQueryError>> errors = response.getInsertErrors();
            List<InsertAllRequest.RowToInsert> failed = new ArrayList<>();
            for (Long idx : errors.keySet()) {
                failed.add(toTry.get(idx.intValue()));
            }
            toTry = failed;
            attempt++;
            try { TimeUnit.MILLISECONDS.sleep(backoff); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            backoff *= 2;
        }

        if (!toTry.isEmpty()) {
            persistToDlq(toTry);
            return false;
        }
        return true;
    }

    private void persistToDlq(List<InsertAllRequest.RowToInsert> rows) {
        if (dlqBucket == null || dlqBucket.isEmpty()) {
            System.err.println("DLQ not configured. Failed rows count: " + rows.size());
            return;
        }
        try {
            String filename = "dlq/failed_" + System.currentTimeMillis() + ".ndjson";
            StringBuilder sb = new StringBuilder();
            com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
            for (InsertAllRequest.RowToInsert r : rows) {
                sb.append(om.writeValueAsString(r.getContent())).append("\n");
            }
            BlobId blobId = BlobId.of(dlqBucket, filename);
            BlobInfo info = BlobInfo.newBuilder(blobId).setContentType("application/x-ndjson").build();
            storage.create(info, sb.toString().getBytes(StandardCharsets.UTF_8));
            System.out.println("Wrote DLQ to gs://" + dlqBucket + "/" + filename);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private long toEpochSec(String ts) {
        if (ts == null) return 0L;
        try {
            return java.time.OffsetDateTime.parse(ts).toEpochSecond();
        } catch (Exception e) {
            try { return java.time.Instant.parse(ts).getEpochSecond(); } catch (Exception ex) { return 0L; }
        }
    }

    private String generateInsertId(MetricEvent e) {
        try {
            String seed = (e.getDevice_name() == null ? "" : e.getDevice_name()) + "|" +
                    (e.getParameter_name() == null ? "" : e.getParameter_name()) + "|" +
                    (e.getTimestamp() == null ? "" : e.getTimestamp()) + "|" +
                    (e.getRaw_value() == null ? "" : e.getRaw_value().toString());
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(seed.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception ex) {
            return UUID.randomUUID().toString();
        }
    }
}

