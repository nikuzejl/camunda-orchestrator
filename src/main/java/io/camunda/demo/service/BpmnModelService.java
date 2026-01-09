package io.camunda.demo.service;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.search.response.MessageSubscription;
import io.camunda.client.api.search.response.ProcessInstance;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.model.bpmn.BpmnModelInstance;
import io.camunda.zeebe.model.bpmn.instance.ReceiveTask;
import io.camunda.zeebe.model.bpmn.instance.zeebe.ZeebeProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static io.camunda.demo.util.StringUtils.stripQuotes;

@Service
public class BpmnModelService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BpmnModelService.class);
    private final CamundaClient camundaClient;
    private final ProcessInstanceService piService;
    private final Map<Long, BpmnModelInstance> cache = new HashMap<>();

    public BpmnModelService(CamundaClient camundaClient, ProcessInstanceService piService) {
        this.camundaClient = camundaClient;
        this.piService = piService;
    }

    public Map<String, String> evaluateReceiveTaskProperties(MessageSubscription sub) {
        BpmnModelInstance model = cache.computeIfAbsent(sub.getProcessDefinitionKey(), this::loadModel);
        ReceiveTask receiveTask = model.getModelElementById(sub.getElementId());
        ZeebeProperties props = receiveTask.getSingleExtensionElement(ZeebeProperties.class);
        Map<String, String> result = new HashMap<>();

        if (props != null) {
            props.getProperties().forEach(p -> {
                String value = p.getValue();

                if (value != null && value.startsWith("$")) {
                    value = piService.loadVariables(sub.getProcessInstanceKey()).get(value.substring(1));
                }

                result.put(p.getName(), value == null ? null : stripQuotes(value));
            });
        }

        return result;
    }

    private BpmnModelInstance loadModel(long defKey) {
        String xml = camundaClient
                .newProcessDefinitionGetXmlRequest(defKey)
                .send()
                .join();

        return Bpmn.readModelFromStream(new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8)));
    }
}
