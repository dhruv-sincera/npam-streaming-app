package com.sincera.npam.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Data
@Getter
@Setter
public class MetricEvent {

    private String device_type;
    private String device_name;
    private String interface_name;
    private String parameter_name;
    private Long raw_value;
    private Double value;
    private String timestamp;

    @JsonProperty("1data-kafka-topic-name")
    private String kafkaTopic;

    @JsonProperty("1data-source-system")
    private String sourceSystem;
}
