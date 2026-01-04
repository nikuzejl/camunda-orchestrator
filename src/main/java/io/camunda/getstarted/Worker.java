package io.camunda.getstarted;

import io.camunda.client.annotation.JobWorker;
import io.camunda.client.annotation.VariablesAsType;
import io.camunda.client.api.response.ActivatedJob;
import io.camunda.client.api.worker.JobClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

@Component
public class Worker {

    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

    @JobWorker(type = "init-variables")
    public Map<String, Object> initialize(
            final ActivatedJob job,
            @VariablesAsType Map<String, Object> allVars
    ) {
        // 1. Zeebe maps "Extension Properties" to Custom Headers
        Map<String, String> properties = job.getCustomHeaders();
        Map<String, Object> evaluatedResults = new HashMap<>();

        properties.forEach((key, value) -> {
            // 2. Evaluation Logic: If value starts with $, look it up
            if (value != null && value.startsWith("$")) {
                String targetVarName = value.substring(1); // Remove the $
                Object actualValue = allVars.getOrDefault(targetVarName, "VARIABLE_NOT_FOUND");
                evaluatedResults.put(key, actualValue);
            } else {
                // 3. Otherwise, keep the static string
                evaluatedResults.put(key, value);
            }
        });

        // Always include the processInstanceKey as we discussed
        evaluatedResults.put("processInstanceKey", job.getProcessInstanceKey());

        return evaluatedResults;
    }

    @JobWorker(type = "my-job")
    public void handle(JobClient client, ActivatedJob job) {
        LOGGER.info("Job key: {}", job.getKey());
        LOGGER.info("Process instance key: {}", job.getProcessInstanceKey());
        LOGGER.info("Tenant: {}", job.getTenantId());

        var variables = job.getVariablesAsMap();
        LOGGER.info("Input variables: {}", variables);

        client.newCompleteCommand(job)
                .variables(Map.of("outputParam", "outputValue"))
                .send()
                .join();
    }

    @JobWorker(type = "long-running-task", autoComplete = false)
    public void startAsyncWork(final ActivatedJob job) {
        long jobKey = job.getKey();

        // Example: Send jobKey to an external system (Message Queue, Third-party API, etc.)
        System.out.println("Initiating work for jobKey: " + jobKey);
        //externalSystem.triggerWork(jobKey, job.getVariablesAsMap());

        // We do NOT call client.newCompleteCommand() here.
        // The job remains 'locked' until the timeout expires or we complete it later.
    }

    @JobWorker(type = "init-receive-task")
    public void initializeReceiveTask(final JobClient client, final ActivatedJob job) {
        // 1. Get the dynamic Process Instance Key
        long piKey = job.getProcessInstanceKey();

        // 2. Prepare the variables you want to attach
        Map<String, Object> localVars = Map.of(
                "extVar_PIK", String.valueOf(piKey),
                "extVar_CreationDate", LocalDateTime.now().toString()
        );

        // 3. Complete the listener.
        // IMPORTANT: Variables set here are now LOCAL to this Receive Task.
        client.newCompleteCommand(job.getKey())
                .variables(localVars)
                .send()
                .join();
    }

}
