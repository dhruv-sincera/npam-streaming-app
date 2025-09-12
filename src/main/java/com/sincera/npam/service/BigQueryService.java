package com.sincera.npam.service;

import com.sincera.npam.config.KafkaBatchListener;
import com.sincera.npam.dto.MetricEvent;
import com.google.cloud.bigquery.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.security.MessageDigest;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.*;

@Service
public class BigQueryService {

    private static final Logger log = LoggerFactory.getLogger(BigQueryService.class);

    private final BigQuery bigQuery;
    private final String dataset;
    private final String table;

    public BigQueryService(BigQuery bigQuery,
                           @Value("${app.bigquery.dataset}") String dataset,
                           @Value("${app.bigquery.table}") String table) {
        this.bigQuery = bigQuery;
        this.dataset = dataset;
        this.table = table;
    }

    /**
     * Inserts a batch of events into BigQuery using insertAll.
     * Returns true when no insert errors (success). If insertAll returns errors, we return false.
     * NOTE: no custom retry or DLQ here (per your constraint).
     */
    public boolean insertRows(List<MetricEvent> events, LookupService lookupService) {
        if (events == null || events.isEmpty()) return true;

        TableId tableId = TableId.of(dataset, table);
        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(tableId);

        for (MetricEvent e : events) {
            Map<String, Object> row = new HashMap<>();

            // timestamp handling: fallback to now() if parse fails
            String ts = e.getTimestamp();
            long epoch = 0L;
            String tsForBq = null;
            if (ts != null) {
                try {
                    OffsetDateTime odt = OffsetDateTime.parse(ts);
                    epoch = odt.toEpochSecond();
                    tsForBq = odt.toString();
                } catch (DateTimeParseException ex) {
                    try {
                        epoch = java.time.Instant.parse(ts).getEpochSecond();
                        tsForBq = java.time.Instant.parse(ts).toString();
                    } catch (Exception ex2) {
                        epoch = java.time.Instant.now().getEpochSecond();
                        tsForBq = java.time.Instant.now().toString();
                    }
                }
            } else {
                epoch = java.time.Instant.now().getEpochSecond();
                tsForBq = java.time.Instant.now().toString();
            }

            row.put("event_timestamp", tsForBq);
            row.put("event_epoch", epoch);
            row.put("collection_interval", "60m"); // default as per spec
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

            // deterministic insertId for idempotency (device|param|timestamp|raw)
            String insertId = generateInsertId(e);
            builder.addRow(insertId, row);
            log.info("bq-done>");
        }

        InsertAllRequest req = builder.build();
        try {
            InsertAllResponse resp = bigQuery.insertAll(req);
            if (resp.hasErrors()) {
                // log details and return false (no retry/dlq per your constraint)
                Map<Long, List<BigQueryError>> errs = resp.getInsertErrors();
                log.error("BigQuery insert errors for batch size " + events.size() + ": " + errs.size());
                errs.forEach((idx, list) -> System.err.println("row idx " + idx + " errors: " + list));
                return false;
            } else {
                return true;
            }
        } catch (BigQueryException ex) {
            // streaming RPC failure — return false and let caller decide (we will not retry here)
            log.error("BigQuery insertAll exception: " + ex.getMessage());
            return false;
        }
    }

    private String generateInsertId(MetricEvent e) {
        try {
            String seed = (e.getDevice_name() == null ? "" : e.getDevice_name()) + "|" +
                    (e.getParameter_name() == null ? "" : e.getParameter_name()) + "|" +
                    (e.getTimestamp() == null ? "" : e.getTimestamp()) + "|" +
                    (e.getRaw_value() == null ? "" : e.getRaw_value().toString());
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(seed.getBytes("UTF-8"));
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) sb.append(String.format("%02x", b));
            return sb.toString();
        } catch (Exception ex) {
            return UUID.randomUUID().toString();
        }
    }
}
