package io.camunda.demo.service;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class FacadeService {
    private static final Logger LOGGER = LoggerFactory.getLogger(FacadeService.class);
    private final DeploymentService deploymentService;
    private final ProcessInstanceService processInstanceService;
    private final MessageService messageService;
    private final MessageSubscriptionInspector messageSubscriptionInspector;

    public FacadeService(
            DeploymentService deploymentService,
            ProcessInstanceService processInstanceService,
            MessageService messageService,
            MessageSubscriptionInspector messageSubscriptionInspector) {
        this.deploymentService = deploymentService;
        this.processInstanceService = processInstanceService;
        this.messageService = messageService;
        this.messageSubscriptionInspector = messageSubscriptionInspector;
    }

    @PostConstruct

    public void init() {
        deploymentService.deploy("TestProcess1.bpmn", null);
        // Start a process instance with initial variables
        messageService.publish("MSG-1", "", true, Map.of("var1", "value1"), null);

        deploymentService.deploy("TestProcess2.bpmn", null);
        // Start a process instance with initial variables
        processInstanceService.start(
                "TestProcess2", null, Map.of("key1", "value1", "key2", "value2"));

        try {
            Thread.sleep(2000); // Wait for the process instance to reach the message subscription point
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("Thread was interrupted", e);
        }

        messageSubscriptionInspector.findAllSubscriptions()
                .forEach(classified -> LOGGER.info(
                        "{} message subscription: name={}, processDefinitionKey={}, processInstanceKey={}, elementId={}, subscriptionKey={}",
                        classified.kind(),
                        classified.subscription().getMessageName(),
                        classified.subscription().getProcessDefinitionKey(),
                        classified.subscription().getProcessInstanceKey(),
                        classified.subscription().getElementId(),
                        classified.subscription().getMessageSubscriptionKey()));
    }
}
