package io.camunda.demo.service;

import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class FacadeService {
    private final DeploymentService deploymentService;
    private final ProcessInstanceService processInstanceService;
    private final MessageService messageService;

    public FacadeService(DeploymentService deploymentService, ProcessInstanceService processInstanceService, MessageService messageService) {
        this.deploymentService = deploymentService;
        this.processInstanceService = processInstanceService;
        this.messageService = messageService;
    }

    @PostConstruct
    public void init() {
        deploymentService.deploy("connectors.bpmn", null);
        processInstanceService.start("connectors", null, Map.of("key1", "value1", "key2", "value2"));

        deploymentService.deploy("test-process.bpmn", null);
        messageService.publish("START-MSG", "123", Map.of("var1", "value2"), null);
    }
}
