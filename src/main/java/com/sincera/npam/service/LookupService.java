package com.sincera.npam.service;

import com.google.cloud.bigquery.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class LookupService {

    private final BigQuery bigQuery;
    private final String lookupTable;
    private final Map<String, String> lookup = new ConcurrentHashMap<>();

    public LookupService(BigQuery bigQuery,
                         @Value("${app.bigquery.lookup-table}") String lookupTable) {
        this.bigQuery = bigQuery;
        this.lookupTable = lookupTable;
    }

    @PostConstruct
    public void loadLookup() {
        String sql = String.format("SELECT source_metric_type, normalized_metric_type FROM `%s`", lookupTable);
        QueryJobConfiguration q = QueryJobConfiguration.newBuilder(sql).setUseLegacySql(false).build();
        try {
            TableResult result = bigQuery.query(q);
            for (FieldValueList row : result.iterateAll()) {
                String src = row.get("source_metric_type").getStringValue();
                String norm = row.get("normalized_metric_type").getStringValue();
                lookup.put(src, norm);
            }
            System.out.println("Loaded metric lookup entries: " + lookup.size());
        } catch (Exception e) {
            throw new RuntimeException("Failed to load lookup table: " + lookupTable, e);
        }
    }

    public String normalize(String sourceMetric) {
        if (sourceMetric == null) return null;
        return lookup.getOrDefault(sourceMetric, sourceMetric);
    }
}
