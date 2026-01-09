package io.camunda.demo.service;

import io.camunda.client.CamundaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class TaskService {
    private static final Logger LOGGER = LoggerFactory.getLogger(TaskService.class);
    private final CamundaClient camundaClient;

    public TaskService(CamundaClient camundaClient) {
        this.camundaClient = camundaClient;
    }

    public void completeTask(Long jobKey, Map<String, Object> variables) {
        camundaClient.newCompleteCommand(jobKey)
                .variables(variables)
                .send()
                .whenComplete((result, exception) -> {
                    if (exception == null) {
                        LOGGER.info("Successfully completed job: {}", jobKey);
                    } else {
                        LOGGER.error("Failed to complete job: {}", exception.getMessage());
                    }
                });
    }
}
