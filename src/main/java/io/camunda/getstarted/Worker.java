package io.camunda.getstarted;

import io.camunda.client.annotation.JobWorker;
import io.camunda.client.api.response.ActivatedJob;
import io.camunda.client.api.worker.JobClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class Worker {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);
    
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

}
