package io.camunda.demo.service;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.search.enums.ProcessInstanceState;
import io.camunda.client.api.search.response.ProcessInstance;
import io.camunda.client.api.search.response.Variable;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static io.camunda.demo.util.Constants.CORRELATION_KEY_VARIABLE;
import static io.camunda.demo.util.StringUtils.stripQuotes;

@Service
public class ProcessInstanceService {
    private final CamundaClient camundaClient;

    public ProcessInstanceService(CamundaClient camundaClient) {
        this.camundaClient = camundaClient;
    }

    public long start(String bpmnProcessId, String tenant, Map<String, Object> variables) {
        Map<String, Object> instanceVariables = new HashMap<>();
        if (variables != null) instanceVariables.putAll(variables);
        instanceVariables.put(CORRELATION_KEY_VARIABLE, UUID.randomUUID().toString());

        var cmd = camundaClient
                .newCreateInstanceCommand()
                .bpmnProcessId(bpmnProcessId)
                .latestVersion()
                .variables(instanceVariables);

        if (tenant != null && !tenant.isBlank()) cmd.tenantId(tenant);

        return cmd.send().join().getProcessInstanceKey();
    }

    public Set<Long> findActiveInstanceKeys() {
        return camundaClient
                .newProcessInstanceSearchRequest()
                .filter(f -> f.state(ProcessInstanceState.ACTIVE))
                .execute()
                .items()
                .stream()
                .map(ProcessInstance::getProcessInstanceKey)
                .collect(Collectors.toSet());
    }

    public Map<String, String> loadVariables(long piKey) {

        return camundaClient.newVariableSearchRequest()
                .filter(f -> f.processInstanceKey(piKey))
                .send()
                .join()
                .items()
                .stream()
                .collect(Collectors.toMap(
                        Variable::getName,
                        v -> stripQuotes(String.valueOf(v.getValue())),
                        (a, b) -> a
                ));
    }
}
