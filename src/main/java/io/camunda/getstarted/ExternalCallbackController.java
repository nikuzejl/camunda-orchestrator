package io.camunda.getstarted;

import io.camunda.client.CamundaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/callback")
public class ExternalCallbackController {

    private final CamundaClient camundaClient;

    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalCallbackController.class);

    public ExternalCallbackController(CamundaClient camundaClient) {
        this.camundaClient = camundaClient;
    }

    @PostMapping("/complete")
    public void handleCallback(@RequestBody CallbackRequest request) {
        camundaClient.newCompleteCommand(request.getJobKey())
                .variables(Map.of("asyncResult", request.getData()))
                .send()
                .whenComplete((result, exception) -> {
                    if (exception == null) {
                        System.out.println("Successfully completed job: " + request.getJobKey());
                    } else {
                        System.err.println("Failed to complete job: " + exception.getMessage());
                    }
                });
    }

    @PostMapping("/message-callback")
    public void handleMessageCallback(@RequestBody CallbackRequest request) {
        // 1. (Optional) Search for the active subscription if you don't know the message name
        // This requires the Search API (available in recent C8 versions)
        camundaClient.newMessageSubscriptionSearchRequest()
                .filter(f -> f.processInstanceKey(request.getJobKey()))
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
}

class CallbackRequest {
    private long jobKey;
    private String data;
    public long getJobKey() { return jobKey; }
    public String getData() { return data; }
}