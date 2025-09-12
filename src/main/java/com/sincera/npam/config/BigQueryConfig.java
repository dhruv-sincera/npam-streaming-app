package com.sincera.npam.config;

import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.FileInputStream;

@Configuration
public class BigQueryConfig {

    @Value("${gcp.project-id}")
    private String projectId;

    @Value("${gcp.credentials-file}")
    private String credentialsFile;

    @Bean
    public BigQuery bigQuery() throws Exception {
        try (FileInputStream fis = new FileInputStream(credentialsFile)) {
            ServiceAccountCredentials creds = ServiceAccountCredentials.fromStream(fis);
            return BigQueryOptions.newBuilder()
                    .setProjectId(projectId)
                    .setCredentials(creds)
                    .build()
                    .getService();
        }
    }
}
