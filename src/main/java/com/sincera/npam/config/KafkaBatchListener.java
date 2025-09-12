package com.sincera.npam.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sincera.npam.dto.MetricEvent;
import com.sincera.npam.service.BigQueryService;
import com.sincera.npam.service.LookupService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

@Component
public class KafkaBatchListener {

    private final ObjectMapper om = new ObjectMapper();
    private final BigQueryService bqService;
    private final LookupService lookupService;

    @Value("${kafka.batch.size:200}")
    private int batchSize;

    @Value("${kafka.batch.poll-timeout-ms:1000}")
    private long pollTimeoutMs;

    public KafkaBatchListener(BigQueryService bqService, LookupService lookupService) {
        this.bqService = bqService;
        this.lookupService = lookupService;
    }

    @KafkaListener(topics = "${kafka.topic}", containerFactory = "kafkaBatchFactory")
    public void listen(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        if (records == null || records.isEmpty()) {
            ack.acknowledge();
            return;
        }

        List<MetricEvent> parsed = new ArrayList<>(records.size());
        for (ConsumerRecord<String, String> rec : records) {
            try {
                MetricEvent e = om.readValue(rec.value(), MetricEvent.class);
                parsed.add(e);
            } catch (Exception ex) {
                // optionally write raw rec.value() to DLQ; for speed we skip here
                System.err.println("Failed to parse message, skipping: " + ex.getMessage());
            }
        }

        if (parsed.isEmpty()) {
            ack.acknowledge();
            return;
        }

        // If parsed size > batchSize, split into chunks
        List<List<MetricEvent>> chunks = partition(parsed, batchSize);

        boolean overallSuccess = true;
        for (List<MetricEvent> chunk : chunks) {
            boolean success = bqService.writeBatch(chunk, lookupService);
            if (!success) overallSuccess = false; // DLQ has been written inside service on permanent failure
        }

        // Acknowledge offsets in all cases (we write failures to DLQ), to avoid stuck offsets.
        ack.acknowledge();

        if (!overallSuccess) {
            // optionally emit a metric/alert
            System.err.println("One or more batches failed and were persisted to DLQ.");
        }
    }

    private List<List<MetricEvent>> partition(List<MetricEvent> list, int size) {
        List<List<MetricEvent>> out = new ArrayList<>();
        for (int i = 0; i < list.size(); i += size) {
            out.add(list.subList(i, Math.min(list.size(), i + size)));
        }
        return out;
    }
}

