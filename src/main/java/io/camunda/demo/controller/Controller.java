package io.camunda.demo.controller;

import java.util.Map;
import io.camunda.demo.service.MessageService;
import io.camunda.demo.service.TaskService;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/")
public class Controller {
    private final MessageService messageService;
    private final TaskService taskService;

    public Controller(MessageService messageService, TaskService taskService) {
        this.messageService = messageService;
        this.taskService = taskService;
    }

    @GetMapping("/complete-task/{jobKey}")
    public void completeTask(@PathVariable Long jobKey) {
        taskService.completeTask(jobKey, Map.of("newVariable", 1151));
    }

    @GetMapping("/message-correlation")
    public void messageCorrelate() {
        messageService.handleExternalMessage("MSG-1", Map.of("key1", "value1","key2", "value2", "abc", "1214", "xyz", "62621"));
    }
}