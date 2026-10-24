# Repository guidance

## Build and test

- Build the application: `./gradlew build`
- Run it: `./gradlew bootRun`
- Run the test suite: `./gradlew test`
- Run one JUnit test class: `./gradlew test --tests 'io.camunda.demo.YourTestClass'`

Tests use JUnit Platform. There are currently no test sources under `src/test`.

The README documents starting a local runtime with `c8run\c8run.exe start`. The application configuration in `src/main/resources/application.yaml` currently selects Camunda SaaS mode; ensure its connection and authentication settings match the runtime being used.

## Architecture

This is a Spring Boot application using the Camunda 8 Spring Boot starter and `CamundaClient`. `ProcessApplication` starts Spring, which wires the services and the annotated job worker.

The service layer separates Camunda operations: `DeploymentService` deploys classpath BPMN resources, `ProcessInstanceService` starts and searches instances and variables, and `MessageService` publishes/correlates messages and filters subscriptions to active instances. `FacadeService` runs a demo sequence in `@PostConstruct`, so application startup itself deploys processes and sends/starts work.

Message matching also depends on the deployed BPMN model. `BpmnModelService` fetches and caches process-definition XML, reads receive-task extension properties, and resolves values prefixed with `$` against process-instance variables. `MessageSubscriptionInspector` searches all message subscriptions in the cluster, paginates through the results, and uses each subscription's process-definition BPMN model to distinguish event-subprocess starts from other instance-scoped subscriptions.

The BPMN definitions in `src/main/resources` provide process IDs, message names, correlation expressions, and job types consumed by the Java services and worker. Keep these values aligned with the corresponding constants and service behavior when changing a process.

## Repository-specific conventions

- `io.camunda.demo.util.Constants` centralizes only the long-running job type and the `correlationKey` variable name. Keep other literals local to the code that uses them.
- Process starts attach a generated `correlationKey` variable. Message publishing places the supplied key in both the Camunda correlation key and that variable; BPMN message subscriptions use the variable in their correlation expressions.
- `BpmnModelService` caches parsed BPMN models by process-definition key. Model-derived receive-task property matching resolves `$variableName` references from the specific process instance.
- Job workers are Spring components registered with `@JobWorker`. The worker's job type must match the BPMN task definition, and manual completion is used when `autoComplete = false`.
