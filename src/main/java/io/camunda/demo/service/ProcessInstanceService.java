package io.camunda.demo.service;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.search.enums.ProcessInstanceState;
import io.camunda.client.api.search.response.ProcessInstance;
import io.camunda.client.api.search.response.Variable;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.camunda.demo.util.StringUtils.stripQuotes;

@Service
public class ProcessInstanceService {
    private final CamundaClient camundaClient;

    public ProcessInstanceService(CamundaClient camundaClient) {
        this.camundaClient = camundaClient;
    }

    public void start(String bpmnProcessId, String tenant, Map<String, Object> variables) {
        var cmd = camundaClient
                .newCreateInstanceCommand()
                .bpmnProcessId(bpmnProcessId)
                .latestVersion()
                .variables(variables);

        if (tenant != null && !tenant.isBlank()) cmd.tenantId(tenant);

        var event = cmd.send().join();

        camundaClient.newSetVariablesCommand(
                event.getProcessInstanceKey())
                .variables(Map.of("processInstanceKey", String.valueOf(event.getProcessInstanceKey())))
                .send()
                .join();
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
