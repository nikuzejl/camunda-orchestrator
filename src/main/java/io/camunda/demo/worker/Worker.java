package io.camunda.demo.worker;

import io.camunda.client.annotation.JobWorker;
import io.camunda.client.api.response.ActivatedJob;
import io.camunda.client.api.worker.JobClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class Worker {
    private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

    @JobWorker(type = "long-running-task", autoComplete = false, timeout = 86400000L)
    public void startAsyncWork(final ActivatedJob job, final JobClient client) {
        long jobKey = job.getKey();
        LOGGER.info("Initiating job for jobKey: {}", jobKey);

        // Perform your work...

//        client.newCompleteCommand(jobKey)
//                .send()
//                .whenComplete((result, exception) -> {
//                    if (exception == null) {
//                        LOGGER.info("Job {} completed successfully", jobKey);
//                    }
//                });
    }
}
