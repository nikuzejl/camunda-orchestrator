## Camunda 8 Demo
- `c8run\c8run.exe start`

## REST call examples
- `curl -X GET http://localhost:8080/v2/topology`
- `curl -X POST http://localhost:8080/v1/process-instances/{instanceKey}/cancel` 
- `curl -X POST http://localhost:8080/v2/process-instances/{instanceKey}/cancellation`

## Others
- Swagger http://localhost:8080/swagger-ui/index.html
- REST specifications https://docs.camunda.io/docs/8.7/apis-tools/camunda-api-rest/specifications/camunda-8-rest-api/  
- Connecting to SaaS cluster https://docs.camunda.io/docs/apis-tools/camunda-spring-boot-starter/configuration/  