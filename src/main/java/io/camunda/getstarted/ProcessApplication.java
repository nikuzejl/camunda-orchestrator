package io.camunda.getstarted;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.search.response.MessageSubscription;
import io.camunda.client.api.search.response.SearchResponse;
import io.camunda.client.api.search.response.Variable;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.util.Map;

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
            deployProcess("test-process.bpmn", null);
            getMessageSubscriptions();
            //publishMessage("START-MSG", "some-key", Map.of("runId", "123"), null);
            startProcessInstance("test-process", null,  Map.of("runId", "123"));
            publishMessage("MSG-1", "123", Map.of("runId", "123"), null);
            publishMessage("MSG-2", "123", Map.of("runId", "123"), null);
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

    private void getMessageSubscriptions() {
        var response = camundaClient
                .newMessageSubscriptionSearchRequest()
                .send()
                .join();

        response.items().forEach(this::logSubscription);
    }

    private void logSubscription(MessageSubscription subscription) {
        LOGGER.info("Message Subscription");
        LOGGER.info("  Subscription key: {}", subscription.getMessageSubscriptionKey());
        LOGGER.info("  Message name: {}", subscription.getMessageName());
        LOGGER.info("  Correlation key: {}", subscription.getCorrelationKey());
        LOGGER.info("  Process instance key: {}", subscription.getProcessInstanceKey());
        LOGGER.info("  Element ID: {}", subscription.getElementId());
        LOGGER.info("--------------------------------------");
    }

    private void logSubscription(MessageSubscription subscription, CamundaClient camundaClient) {
        long piKey = subscription.getProcessInstanceKey();
        LOGGER.info("Fetching variables for Process Instance: {}", piKey);
        SearchResponse<Variable> response = camundaClient
                .newVariableSearchRequest()
                .filter(f -> f.processInstanceKey(piKey))
                .send()
                .join();

        if (response.items() != null) {
            response.items().forEach(var ->
                    LOGGER.info("  Variable: {} = {}", var.getName(), var.getValue())
            );
        }
    }
}
