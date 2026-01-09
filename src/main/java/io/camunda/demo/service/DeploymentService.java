package io.camunda.demo.service;

import io.camunda.client.CamundaClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class DeploymentService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeploymentService.class);
    private final CamundaClient camundaClient;

    public DeploymentService(CamundaClient camundaClient) {
        this.camundaClient = camundaClient;
    }

    public void deploy(String resource, String tenant) {
        var cmd = camundaClient.newDeployResourceCommand().addResourceFromClasspath(resource);

        if (tenant != null && !tenant.isBlank()) {
            cmd.tenantId(tenant);
        }

        var deployment = cmd.send().join();
        LOGGER.info("Deployment successful: {}", deployment.getKey());

        deployment.getProcesses().forEach(p ->
                LOGGER.info("Deployed {} v{} (tenant={})", p.getBpmnProcessId(), p.getVersion(), p.getTenantId())
        );
    }
}
