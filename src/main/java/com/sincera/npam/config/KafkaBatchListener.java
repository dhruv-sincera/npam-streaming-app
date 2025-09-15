package com.sincera.npam.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sincera.npam.dto.MetricEvent;
import com.sincera.npam.service.BigQueryService;
import com.sincera.npam.service.LookupService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class KafkaBatchListener {

    private static final Logger log = LoggerFactory.getLogger(KafkaBatchListener.class);

    private final ObjectMapper om = new ObjectMapper();
    private final BigQueryService bqService;
    private final LookupService lookupService;
    private final PipelineMetrics metrics;

    @Value("${kafka.batch.size:2000}")
    private int batchSize;

    public KafkaBatchListener(BigQueryService bqService, LookupService lookupService, PipelineMetrics metrics) {
        this.bqService = bqService;
        this.lookupService = lookupService;
        this.metrics = metrics;
    }

    @KafkaListener(id = "${kafka.group-id}", topics = "${kafka.topic}", containerFactory = "kafkaBatchFactory")
    public void listen(List<ConsumerRecord<String, String>> records, Acknowledgment ack) {
        if (records == null || records.isEmpty()) {
            ack.acknowledge();
            return;
        }

        List<MetricEvent> parsed = new ArrayList<>(records.size());
        for (ConsumerRecord<String, String> rec : records) {
            try {
                MetricEvent e = om.readValue(rec.value(), MetricEvent.class);
                log.info("kafka-done>");
                parsed.add(e);
                metrics.incrementConsumed(records.size());
            } catch (Exception ex) {
                log.error("Failed to parse message: " + ex.getMessage());
            }
        }

        if (parsed.isEmpty()) {
            ack.acknowledge();
            return;
        }

        // partition to batches
        List<List<MetricEvent>> chunks = partition(parsed, batchSize);

        boolean allSuccess = true;
        for (List<MetricEvent> chunk : chunks) {
            boolean ok = bqService.insertRows(chunk, lookupService);
            if (!ok) {
                allSuccess = false;
                log.error("BigQuery insert failed for a chunk of size " + chunk.size() + ". Will not ack offsets; Kafka will re-deliver.");
                break;
            }
        }

        if (allSuccess) {
            ack.acknowledge();
            metrics.incrementPushed(records.size());
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
