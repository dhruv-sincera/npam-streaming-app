package com.sincera.npam.service;

import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Service;

@Service
public class KafkaControlService {

    private final KafkaListenerEndpointRegistry registry;

    public KafkaControlService(KafkaListenerEndpointRegistry registry) {
        this.registry = registry;
    }

    public void start() {
        registry.getListenerContainers()
                .forEach(MessageListenerContainer::start);
    }

    public void stop() {
        registry.getListenerContainers()
                .forEach(MessageListenerContainer::stop);
    }

    public boolean isRunning() {
        return registry.getListenerContainers()
                .stream().anyMatch(MessageListenerContainer::isRunning);
    }
}
