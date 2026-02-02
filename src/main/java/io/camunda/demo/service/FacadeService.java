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

        processInstanceService.start("test-process", null, Map.of("key1", "value1", "key2", "value2"));
        processInstanceService.start("test-process", null, Map.of("key1", "value113141", "key2", "value252661"));
        processInstanceService.start("test-process", null, Map.of("key1", "va631q41", "key2", "value2"));

        messageService.publish("START-MSG", "123", Map.of("var1", "value2"), null);

        int N = 1;
        for (int i = 0; i< N; i ++){
            processInstanceService.start("test-process", null, Map.of("key1", "va631q41", "key2", "value2"));
            System.out.println("Started process instance " + i);
        }
    }
}
