package io.camunda.demo.service;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.search.response.MessageSubscription;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.BpmnModelElementInstance;
import io.camunda.zeebe.model.bpmn.instance.StartEvent;
import io.camunda.zeebe.model.bpmn.instance.SubProcess;
import org.camunda.bpm.model.xml.instance.ModelElementInstance;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service
public class MessageSubscriptionInspector {
    private final CamundaClient camundaClient;

    public MessageSubscriptionInspector(CamundaClient camundaClient) {
        this.camundaClient = camundaClient;
    }

    public List<ClassifiedSubscription> findAllSubscriptions() {
        List<MessageSubscription> subscriptions = findAllSubscriptionsInCluster();
        Map<Long, BpmnModelInstance> modelsByDefinitionKey = new HashMap<>();

        return subscriptions.stream()
                .map(subscription -> classify(subscription, modelsByDefinitionKey))
                .toList();
    }

    private List<MessageSubscription> findAllSubscriptionsInCluster() {
        List<MessageSubscription> subscriptions = new ArrayList<>();
        String cursor = null;

        do {
            var request = camundaClient.newMessageSubscriptionSearchRequest();
            if (cursor == null) {
                request.page(page -> page.limit(100));
            } else {
                String pageCursor = cursor;
                request.page(page -> page.after(pageCursor).limit(100));
            }

            var response = request.send().join();
            subscriptions.addAll(response.items());
            cursor = Boolean.TRUE.equals(response.page().hasMoreTotalItems())
                    ? response.page().endCursor()
                    : null;
            if (Boolean.TRUE.equals(response.page().hasMoreTotalItems())
                    && (cursor == null || cursor.isBlank())) {
                throw new IllegalStateException(
                        "Message subscription search has more results but returned no end cursor");
            }
        } while (cursor != null);

        return subscriptions;
    }

    /**
     * START_EVENT subscriptions are definition-scoped (process start events). PROCESS_EVENT
     * subscriptions are instance-scoped and cover both event subprocess start events and
     * intermediate catch events, so the BPMN model is needed to tell those apart.
     */
    private ClassifiedSubscription classify(
            MessageSubscription subscription, Map<Long, BpmnModelInstance> modelsByDefinitionKey) {
        SubscriptionKind kind = switch (subscription.getMessageSubscriptionType()) {
            case START_EVENT -> SubscriptionKind.PROCESS_START_MESSAGE;
            case PROCESS_EVENT -> isEventSubprocessStart(
                    subscription,
                    modelsByDefinitionKey.computeIfAbsent(
                            subscription.getProcessDefinitionKey(), this::loadProcessModel))
                    ? SubscriptionKind.EVENT_SUBPROCESS_START_MESSAGE
                    : SubscriptionKind.OTHER_MESSAGE_SUBSCRIPTION;
            default -> SubscriptionKind.OTHER_MESSAGE_SUBSCRIPTION;
        };

        return new ClassifiedSubscription(subscription, kind);
    }

    private boolean isEventSubprocessStart(
            MessageSubscription subscription, BpmnModelInstance processModel) {
        BpmnModelElementInstance element = processModel.getModelElementById(subscription.getElementId());
        return element instanceof StartEvent && hasEventSubprocessAncestor(element);
    }

    private boolean hasEventSubprocessAncestor(ModelElementInstance element) {
        ModelElementInstance parent = element.getParentElement();
        while (parent != null) {
            if (parent instanceof SubProcess subProcess && subProcess.triggeredByEvent()) {
                return true;
            }
            parent = parent.getParentElement();
        }
        return false;
    }

    private BpmnModelInstance loadProcessModel(long processDefinitionKey) {
        String xml = camundaClient.newProcessDefinitionGetXmlRequest(processDefinitionKey)
                .send()
                .join();
        return Bpmn.readModelFromStream(
                new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    }

    public enum SubscriptionKind {
        PROCESS_START_MESSAGE,
        EVENT_SUBPROCESS_START_MESSAGE,
        OTHER_MESSAGE_SUBSCRIPTION
    }

    public record ClassifiedSubscription(
            MessageSubscription subscription, SubscriptionKind kind) {}
}
