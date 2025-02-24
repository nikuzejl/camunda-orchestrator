## Instructions to start Camunda 8 on local
- https://camunda.com/blog/2023/10/camunda-self-managed-for-absolute-beginners/

## Running C8 cluster
- For the first time, follow instruction the c8-self-managed folder
- Start Docker desktop
- Runner Docker images
- Operate http://localhost:8081
- Tasklist http://localhost:8082/
- Cluster endpoint/gateway http://0.0.0.0:26500

## Downloading Camunda 8 Run
### Linux
- `sudo apt install openjdk-21-jdk`  
- `echo 'export JAVA_HOME="/usr"' >> ~/.bashrc`
- `echo 'export ES_JAVA_HOME="/usr"' >> ~/.bashrc`  
- `wget https://github.com/camunda/camunda/releases/download/c8run-8.6.2/camunda8-run-8.6.2-linux-x86_64.tar.gz`
- `tar -xvzf camunda8-run-8.6.2-linux-x86_64.tar.gz`

### Windows
- Install JDK 21+
- Set environment variables JAVA_HOME and JAVA_VERSION
- Dowload https://github.com/camunda/camunda/releases/tag/c8run-8.6.2
- Extract the archive

## Starting Camunda 8
### Linux
- `source ~/Desktop/Software/c8run/start.sh`

### Windows
- `cd C:\Users\Masabo\Desktop\Software\c8run`
- `c8run.exe start`

## Starting Kafka on Windows
- `cd C:\Users\%USERNAME%\kafka`
- `C:\Users\%USERNAME%\kafka\bin\windows\zookeeper-server-start.bat C:\Users\%USERNAME%\kafka\config\zookeeper.properties`
- `C:\Users\%USERNAME%\kafka\bin\windows\kafka-server-start.bat C:\Users\%USERNAME%\kafka\config\server.properties`

- `C:\Users\%USERNAME%\kafka\bin\windows\kafka-console-producer.bat --broker-list localhost:9092 --topic my-topic`
- `C:\Users\%USERNAME%\kafka\bin\windows\kafka-console-consumer.bat --bootstrap-server localhost:9092 --topic my-topic --from-beginning`  

## Starting Kafka on Linux
- `cd ~/Desktop/kafka`
- `~/Desktop/Software/kafka/bin/zookeeper-server-start.sh ~/Desktop/Software/kafka/config/zookeeper.properties`
- `~/Desktop/Software/kafka/bin/kafka-server-start.sh ~/Desktop/Software/kafka/config/server.properties`

- `~/Desktop/Software/kafka/bin/kafka-console-producer.sh --broker-list localhost:9092 --topic my-topic`
- `~/Desktop/Software/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic my-topic --from-beginning`

# TO-DO
- Kafka
- gRPC
- REST
- LDAP

# Learning resources
- https://github.com/camunda-community-hub/kafka-connect-zeebe/blob/main/docker/docker-compose-local-kafka.yml
- https://www.youtube.com/watch?v=SqVfCyfCJqw
- https://www.youtube.com/watch?v=BwYFuhVhshI&t=467s

## Camunda Platform 8 - Get Started - Spring Boot

This guide explains how to setup a Spring Boot project to automate a process using
[Camunda Platform 8](https://camunda.com/products/cloud/).

# Install dependencies

The [official Spring Zeebe SDK](https://docs.camunda.io/docs/next/apis-tools/spring-zeebe-sdk/getting-started/)
provides a Zeebe client.

```
		<dependency>
			<groupId>io.camunda</groupId>
			<artifactId>spring-boot-starter-camunda-sdk</artifactId>
			<version>${zeebe.version}</version>
		</dependency>
```

# Create Client

If we want to connect to a Camunda Platform 8 SaaS cluster we need the `clusterId` from the 
[Clusters details page](https://docs.camunda.io/docs/components/console/manage-clusters/create-cluster/),
a `clientId` and `clientSecret` from a [client credentials pair](https://docs.camunda.io/docs/components/console/manage-clusters/manage-api-clients/). 

The credentails can be added to the application.properties.

```properties
zeebe.client.cloud.clusterId=365eed98-16c1-4096-bb57-eb8828ed131e
zeebe.client.cloud.clientId=GZVO3ALYy~qCcD3MYq~sf0GIszNzLE_z
zeebe.client.cloud.clientSecret=.RPbZc6q0d6uzRbB4LW.B8lCpsxbBEpmBX0AHQGzINf3.KK9RkzZW1aDaZ-7WYNJ
```

If you are using a self managed Camunda Platform 8 cluster, you create the client
using the following application config, see
[application.localhost.yaml](src/main/resources/application.localhost.properties).

```properties
zeebe.client.broker.gatewayAddress=127.0.0.1:26500
zeebe.client.security.plaintext=true
```

To enable the Zeebe client integration annotate your application class with
`@EnableZeebeClient`, see
[ProcessApplication.java](src/main/java/io/camunda/getstarted/ProcessApplication.java).

```java
@SpringBootApplication
@EnableZeebeClient
public class ProcessApplication {

  public static void main(String[] args) {
    SpringApplication.run(ProcessApplication.class, args);
  }

}
```

# Deploy Process and Start Instance

To deploy a process you can use the annotation `@Deployment`, which allows
to specify a list of `resources` (e.g. from classpath) to be deployed on start up.

```java
@SpringBootApplication
@EnableZeebeClient
@Deployment(resources = "classpath:send-email.bpmn")
public class ProcessApplication {

  public static void main(String[] args) {
    SpringApplication.run(ProcessApplication.class, args);
  }

}
```

To start a new instance you can specify the `bpmnProcessId`, i.e.
`send-email` and **optionally** process variables.

```java
final ProcessInstanceEvent event =
  client
    .newCreateInstanceCommand()
    .bpmnProcessId("send-email")
    .latestVersion()
    .variables(Map.of("message_content", "Hello from the Spring Boot get started"))
    .send()
    .join();

LOG.info("Started instance for processDefinitionKey='{}', bpmnProcessId='{}', version='{}' with processInstanceKey='{}'",
	event.getProcessDefinitionKey(), event.getBpmnProcessId(), event.getVersion(), event.getProcessInstanceKey());
```

For the complete code see the
[`ProcessApplication.java`](src/main/java/io/camunda/getstarted/ProcessApplication.java) file. You can
run it using the following command.

```bash
mvn spring-boot:run
```

# Job Worker

To complete a
[service task](https://docs.camunda.io/docs/reference/bpmn-workflows/service-tasks/service-tasks/),
a [job worker](https://docs.camunda.io/docs/product-manuals/concepts/job-workers) has
to be subscribed the to task type defined in the process, i.e. `email`. For this
the `@JobWorker` annotation can be used and the `type` has to be specified.

```
@JobWorker(type = "email")
public void sendEmail(final ActivatedJob job) {
  final String message_content = (String) job.getVariablesAsMap().get("message_content");
  LOG.info("Sending email with message content: {}", message_content);
}
```

For the complete code see the
[EmailWorker.java](src/main/java/io/camunda/getstarted/EmailWorker.java) file. You can
run it using the following command.

```bash
mvn spring-boot:run
```

To make an job available, a user task has to be completed, follow the
instructions in [the guide](../README.md#complete-the-user-task).


# Blocking vs. Non-Blocking Code

The code example to start a process instance used 
```
send().join()
```
which is a blocking call to wait for the issues command to be executed on the workflow engine. 
While this is very straightforward to use and produces easy-to-read code, 
blocking code is limited in terms of scalability. 

This is discussed in more detail in [this blog post about writing good workers for Camunda Platform 8](https://blog.bernd-ruecker.com/writing-good-workers-for-camunda-cloud-61d322cad862).
