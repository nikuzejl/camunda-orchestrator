package io.camunda.demo.service;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.search.response.MessageSubscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

import static io.camunda.demo.util.Constants.CORRELATION_KEY_VARIABLE;

@Service
public class MessageService {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageService.class);
    private final CamundaClient camundaClient;
    private final ProcessInstanceService piService;
    private final BpmnModelService bpmnModelService;

    public MessageService(
            CamundaClient camundaClient,
            ProcessInstanceService piService,
            BpmnModelService bpmnModelService) {
        this.camundaClient = camundaClient;
        this.piService = piService;
        this.bpmnModelService = bpmnModelService;
    }

    public void publish(String name, String correlationKey, Boolean isProcessStartEvent, Map<String, Object> vars, String tenant) {
        HashMap<String, Object> variables = vars != null ? new HashMap<>(vars) : new HashMap<>();

        if (isProcessStartEvent){
            variables.put(CORRELATION_KEY_VARIABLE, UUID.randomUUID().toString());
        }
        var cmd = camundaClient.newPublishMessageCommand()
                .messageName(name)
                .correlationKey(correlationKey)
                .variables(variables);

        if (tenant != null && !tenant.isBlank()) cmd.tenantId(tenant);
        cmd.send().join();
    }

    public List<MessageSubscription> findActiveSubscriptions(String messageName) {
        List<MessageSubscription> subs = camundaClient
                        .newMessageSubscriptionSearchRequest()
                        .filter(f -> f.messageName(messageName))
                        .send()
                        .join()
                        .items();

        Set<Long> activeKeys = piService.findActiveInstanceKeys();

        return subs.stream().filter(s ->
                activeKeys.contains(s.getProcessInstanceKey()))
                .toList();
    }

    public void handleExternalMessage(String messageName, Map<String, String> messageDetails) {
        List<MessageSubscription> subscriptions = findActiveSubscriptions(messageName);

        subscriptions.forEach(subscription -> {
            Map<String, String> msgLocalVariables = bpmnModelService.evaluateReceiveTaskProperties(subscription);

            boolean match = msgLocalVariables.entrySet().stream()
                    .allMatch(e -> Objects.equals(messageDetails.get(e.getKey()), e.getValue()));

            if (match) {
                Long piKey = subscription.getProcessInstanceKey();
                LOGGER.info("Matched message subscription in process instance {}", piKey);

                camundaClient
                        .newCorrelateMessageCommand()
                        .messageName(messageName)
                        .correlationKey(subscription.getCorrelationKey())
                        .send();
            }
            else{
                LOGGER.warn("Ignoring message {} wit details {} because it does not match any active instance keys", messageName, messageDetails);
            }
        });
    }

    public void correlateMessage(String name, String key, Map<String, Object> vars) {
        camundaClient.newCorrelateMessageCommand()
                .messageName(name)
                .correlationKey(key)
                .variables(vars)
                .send()
                .whenComplete((res, ex) -> {
                    if (ex == null) {
                        LOGGER.info("Successfully correlated message to instance {}", res.getProcessInstanceKey());
                    }
                });
    }
}
