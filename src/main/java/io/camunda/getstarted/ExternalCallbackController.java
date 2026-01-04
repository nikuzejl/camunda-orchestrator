package io.camunda.getstarted;

import io.camunda.client.CamundaClient;
import io.camunda.client.api.search.enums.ProcessInstanceState;
import io.camunda.client.api.search.response.*;
import io.camunda.client.api.search.filter.*;
import io.camunda.client.api.search.filter.VariableValueFilter; // Check this import
import io.camunda.client.api.search.response.ProcessInstance;
import io.camunda.client.api.search.response.SearchResponse;

import java.util.*;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/")
public class ExternalCallbackController {

    private final CamundaClient camundaClient;

    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalCallbackController.class);

    public ExternalCallbackController(CamundaClient camundaClient) {
        this.camundaClient = camundaClient;
    }

    @PostMapping("/complete-task")
    public void completeTask(@RequestBody Request request) {
        camundaClient.newCompleteCommand(request.getData())
                .variables(Map.of("asyncResult", request.getData()))
                .send()
                .whenComplete((result, exception) -> {
                    if (exception == null) {
                        System.out.println("Successfully completed job: " + request.getData());
                    } else {
                        System.err.println("Failed to complete job: " + exception.getMessage());
                    }
                });
    }

    @PostMapping("/message-callback")
    public void messageCallback(@RequestBody Request request) {
        camundaClient.newMessageSubscriptionSearchRequest()
                .filter(f -> f.processInstanceKey(request.getData()))
                .send()
                .whenComplete((searchResult, searchEx) -> {
                    if (searchEx == null && !searchResult.items().isEmpty()) {
                        var subscription = searchResult.items().get(0);

                        // 2. Correlate the message using the data from the subscription
                        correlate(subscription.getMessageName(),
                                subscription.getCorrelationKey(),
                                Map.of("newVar", "value"));
                    }
                });
    }

    private void correlate(String name, String key, Map<String, Object> vars) {
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

    @GetMapping("simulate-message-sending")
    public void simulateMessageSending() {
        searchAndCorrelate("MSG-1", Map.of("key1", "value1","key2", "value2"));
        searchAndCorrelate("MSG-2", Map.of("key1", "newValue1","key2", "newValue2"));
    }

    public void searchAndCorrelate(
            String messageName,
            Map<String, String> searchCriteria
    ) {

        camundaClient
                .newProcessInstanceSearchRequest()
                .filter(f -> f.state(ProcessInstanceState.valueOf("ACTIVE")))
                .execute()
                .items()
                .forEach(pi -> {

                    long processInstanceKey = pi.getProcessInstanceKey();

                    // Fetch variables for this process instance
                    var variableResponse = camundaClient
                            .newVariableSearchRequest()
                            .filter(f -> f.processInstanceKey(processInstanceKey))
                            .send()
                            .join();

                    // Convert variables to a Map<String, String>
                    Map<String, String> vars = variableResponse.items().stream()
                            .collect(Collectors.toMap(
                                    Variable::getName,
                                    v -> String.valueOf(v.getValue()),
                                    (a, b) -> a
                            ));

                   // Remove extra quotes from string values
                    vars.replaceAll((k, v) ->
                            v != null && v.length() >= 2 && v.startsWith("\"") && v.endsWith("\"")
                                    ? v.substring(1, v.length() - 1)
                                    : v
                    );

                    // Check if searchCriteria is a submap of vars
                    boolean isSubMap = searchCriteria.entrySet().stream()
                            .allMatch(e -> Objects.equals(vars.get(e.getKey()), e.getValue()));

                    if (isSubMap) {
                        camundaClient
                                .newCorrelateMessageCommand()
                                .messageName(messageName)
                                .correlationKey(String.valueOf(processInstanceKey))
                                .variables(Map.of("callbackData", "processed"))
                                .send()
                                .whenComplete((res, ex) -> {
                                    if (ex == null) {
                                        System.out.println(
                                                "Message correlated successfully to process instance key: "
                                                        + processInstanceKey
                                        );
                                    } else {
                                        ex.printStackTrace();
                                    }
                                });
                    }
                });
    }
}