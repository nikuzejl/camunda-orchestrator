package io.camunda.getstarted;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.search.enums.ProcessInstanceState;
import io.camunda.client.api.search.response.MessageSubscription;
import io.camunda.client.api.search.response.ProcessInstance;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.ReceiveTask;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeProperties;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@SpringBootApplication
public class ProcessApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProcessApplication.class);

    @Autowired
    private CamundaClient camundaClient;

    public static void main(String[] args) {
        SpringApplication.run(ProcessApplication.class, args);
    }

    @PostConstruct
    public void start() {
        try {
            //deployProcess("test-process.bpmn", null);
            getMessageSubscriptions(Map.of("extVar1", "newValue1","extVar2", "newValue2"));
            //publishMessage("START-MSG", "some-key", Map.of("runId", "123"), null);
            //startProcessInstance("test-process", null,  Map.of("key1", "value1","key2", "value2"));
            //startProcessInstance("test-process", null,  Map.of("key1", "newValue1","key2", "newValue2"));
            deployProcess("test-process.bpmn", null);
            startProcessInstance("test-process", null,  Map.of("key1", "newValue1","key2", "newValue2"));
            publishMessage("MSG-1", "abc", Map.of("runId", "123"), null);
            publishMessage("MSG-2", "abc", Map.of("runId", "123"), null);
            startVsMiddleMessageSubscription("MSG-1");
            startVsMiddleMessageSubscription("TEST-MSG");
        } catch (Exception e) {
            LOGGER.error("Startup failure", e);
        }
    }

    private void deployProcess(String processDefinition, String tenant) {
        var command = camundaClient.newDeployResourceCommand()
                .addResourceFromClasspath(processDefinition);

        if (tenant != null && !tenant.isBlank()) {
            command.tenantId(tenant);
        }

        var deployment = command.send().join();

        LOGGER.info("Deployment successful: {}", deployment.getKey());

        deployment.getProcesses().forEach(p ->
                LOGGER.info("Deployed process: {} v{} (Tenant: {})",
                        p.getBpmnProcessId(),
                        p.getVersion(),
                        p.getTenantId()
                )
        );
    }

    private void publishMessage(
            String messageName,
            String correlationKey,
            Map<String, Object> variables,
            String tenant
    ) {
        var command = camundaClient.newPublishMessageCommand()
                .messageName(messageName)
                .correlationKey(correlationKey)
                .variables(variables);

        if (tenant != null && !tenant.isBlank()) {
            command.tenantId(tenant);
        }

        command.send().join();

        LOGGER.info(
                "Message published: name={}, correlationKey={}, tenant={}",
                messageName,
                correlationKey,
                tenant != null ? tenant : "<default>"
        );
    }

    public List<MessageSubscription> getValidActiveSubscriptions(String messageName) {
        // 1. Get all potential subscriptions
        var subscriptions = camundaClient.newMessageSubscriptionSearchRequest()
                .filter(f -> f.messageName(messageName))
                .send()
                .join()
                .items();

        if (subscriptions.isEmpty()) return List.of();

        // 2. Get the unique Process Instance Keys from these subscriptions
        List<Long> piKeys = subscriptions.stream()
                .map(MessageSubscription::getProcessInstanceKey)
                .distinct()
                .toList();

        // 3. Query the Process Instance index for only ACTIVE ones
        List<Long> activePiKeys = camundaClient
                .newProcessInstanceSearchRequest()
                .filter(f -> f.state(ProcessInstanceState.valueOf("ACTIVE")))
                .execute()
                .items().stream().map(ProcessInstance::getProcessInstanceKey).toList();

        // 4. Return only subscriptions where the Process Instance is confirmed ACTIVE
        return subscriptions.stream()
                .filter(s -> activePiKeys.contains(s.getProcessInstanceKey()))
                .toList();
    }

    public void startVsMiddleMessageSubscription(String messageName){
//        var subscriptions = camundaClient.newMessageSubscriptionSearchRequest()
//                .filter(f -> f.messageName(messageName))
//                .send()
//                .join();

        var subscriptions = getValidActiveSubscriptions(messageName);

        subscriptions.forEach(sub -> {
            if (sub.getProcessInstanceKey() > 0) {
                System.out.println("Instance " + sub.getProcessInstanceKey() + " is waiting at " + sub.getElementId());
            } else {
                System.out.println("This message will trigger a Start Event for definition " + sub.getProcessDefinitionKey());
            }
        });
    }

    private void startProcessInstance(String bpmnProcessId, String tenant, Map<String, Object> variables) {
        var command = camundaClient.newCreateInstanceCommand()
                .bpmnProcessId(bpmnProcessId)
                .latestVersion()
                .variables(variables);

        if (tenant != null && !tenant.isBlank()) {
            command.tenantId(tenant);
        }

        var event = command.send().join();

        long piKey = event.getProcessInstanceKey();
        camundaClient.newSetVariablesCommand(piKey)
                .variables(Map.of("processInstanceKey", String.valueOf(piKey)))
                .send().join();

        LOGGER.info("Started instance for process {} in tenant {}. Instance Key: {}",
                event.getBpmnProcessId(),
                event.getTenantId(),
                event.getProcessInstanceKey());
    }

    public void getMessageSubscriptions(Map<String, String> variablesToMatch) {
        var response = camundaClient
                .newMessageSubscriptionSearchRequest()
                .send()
                .join();

        response.items().forEach(subscription -> {
            logSubscription(subscription);

            // 1. Extract and evaluate properties from the BPMN XML
            Map<String, String> localVariables = getReceiveTaskExtensionProperties(subscription);

            // 2. Check if all 'variablesToMatch' are present and have the same values
            boolean isMatch = variablesToMatch.entrySet().stream()
                    .allMatch(entry -> {
                        String actualValue = localVariables.get(entry.getKey());

                        // Note: Handle potential nulls and JSON quotes if actualValue comes from Variable API
                        if (actualValue == null) return false;

                        return actualValue.equals(entry.getValue());
                    });

            // 3. Log success if conditions are met
            if (isMatch && !variablesToMatch.isEmpty()) {
                LOGGER.info("SUCCESS: Subscription for Element {} matches provided criteria.",
                        subscription.getElementId());
            }
            else{
                LOGGER.error("Variables did not match for {}", subscription.getElementId());
            }
        });
    }

    private void fetchVariablesForSubscription(long elementInstanceKey) {
        var variablesResponse = camundaClient
                .newVariableSearchRequest()
                .filter(f -> f.scopeKey(elementInstanceKey))
                .send()
                .join();

        variablesResponse.items().forEach(variable -> {
            System.out.println("Variable Name: " + variable.getName());
            System.out.println("Variable Value: " + variable.getValue());
        });
    }

    private Map<String, String> getReceiveTaskExtensionProperties(MessageSubscription subscription) {
        long processDefinitionKey = subscription.getProcessDefinitionKey();
        String xml = camundaClient
                .newProcessDefinitionGetXmlRequest(processDefinitionKey)
                .send()
                .join();

        InputStream stream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
        BpmnModelInstance modelInstance = Bpmn.readModelFromStream(stream);

        ReceiveTask receiveTask = modelInstance.getModelElementById("msg-1");
        ZeebeProperties extensionElements = receiveTask.getSingleExtensionElement(ZeebeProperties.class);

        Map<String, String> evaluatedVariables = new HashMap<>();

        if (extensionElements != null) {
            extensionElements.getProperties().forEach(p -> {
                String rawValue = p.getValue();
                String finalValue;

                if (rawValue != null && rawValue.startsWith("$")) {
                    String varName = rawValue.substring(1);
                    finalValue = fetchVariableValue(subscription.getProcessInstanceKey(), varName).toString();
                } else {
                    finalValue = rawValue;
                }

                evaluatedVariables.put(p.getName(), removeQuotes(finalValue));
                System.out.println("Property: " + p.getName() + " = " + removeQuotes(finalValue));
            });
        }
        return evaluatedVariables;
    }

    private String removeQuotes(String input) {
        return input.replace("\"", "");
    }

    // Helper method to query the variable state
    private Object fetchVariableValue(long processInstanceKey, String variableName) {
        try {
            var response = camundaClient.newVariableSearchRequest()
                    .filter(f -> f.processInstanceKey(processInstanceKey).name(variableName))
                    .send()
                    .join();

            if (!response.items().isEmpty()) {
                // Note: The value is returned as a JSON string (e.g., "\"value\"" or "123")
                return response.items().get(0).getValue();
            }
        } catch (Exception e) {
            System.err.println("Could not fetch variable " + variableName + ": " + e.getMessage());
        }
        return "NOT_FOUND";
    }

    private void logSubscription(MessageSubscription subscription) {
        long elementInstanceKey = subscription.getElementInstanceKey();
        fetchVariablesForSubscription(elementInstanceKey);

        LOGGER.info("Message Subscription");
        LOGGER.info("  Subscription key: {}", subscription.getMessageSubscriptionKey());
        LOGGER.info("  Message name: {}", subscription.getMessageName());
        LOGGER.info("  Correlation key: {}", subscription.getCorrelationKey());
        LOGGER.info("  Process instance key: {}", subscription.getProcessInstanceKey());
        LOGGER.info("  Element ID: {}", subscription.getElementId());
        LOGGER.info("--------------------------------------");
    }
}
