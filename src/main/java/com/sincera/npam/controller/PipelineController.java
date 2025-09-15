package com.sincera.npam.controller;

import com.sincera.npam.config.PipelineMetrics;
import com.sincera.npam.service.KafkaControlService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/npam")
public class PipelineController {

    private final KafkaControlService controlService;
    private final PipelineMetrics metrics;

    public PipelineController(KafkaControlService controlService, PipelineMetrics metrics) {
        this.controlService = controlService;
        this.metrics = metrics;
    }

    @PostMapping("/start")
    public String start() {
        controlService.start();
        return "Pipeline started";
    }

    @PostMapping("/stop")
    public String stop() {
        controlService.stop();
        return "Pipeline stopped";
    }

    @GetMapping("/status")
    public Map<String, Object> status() {
        Map<String, Object> map = new HashMap<>();
        map.put("running", controlService.isRunning());
        map.put("consumed", metrics.getConsumed());
        map.put("pushed", metrics.getPushed());
        return map;
    }
}
