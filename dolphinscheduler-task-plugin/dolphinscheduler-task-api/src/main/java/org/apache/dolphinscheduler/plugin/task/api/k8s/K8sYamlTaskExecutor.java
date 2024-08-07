/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.plugin.task.api.k8s;

import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.K8sTaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.K8sYamlType;
import org.apache.dolphinscheduler.plugin.task.api.enums.TaskTimeoutStrategy;
import org.apache.dolphinscheduler.plugin.task.api.k8s.impl.K8sPodOperation;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;
import org.apache.dolphinscheduler.plugin.task.api.parameters.K8sYamlContentDTO;
import org.apache.dolphinscheduler.plugin.task.api.parser.TaskOutputParameterParser;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.MapUtils;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.dsl.LogWatch;

/**
 * K8sYamlTaskExecutor submits customized YAML k8s task to Kubernetes
 */
@Slf4j
public class K8sYamlTaskExecutor extends AbstractK8sTaskExecutor {

    private HasMetadata metadata;
    private K8sYamlType k8sYamlType;
    protected boolean podLogOutputIsFinished = false;
    protected Future<?> podLogOutputFuture;
    private AbstractK8sOperation abstractK8sOperation;

    public K8sYamlTaskExecutor(TaskExecutionContext taskRequest) {
        super(taskRequest);
    }

    /**
     * Executes a task based on the provided Kubernetes parameters.
     *
     * <p>This method processes the YAML content describing the Kubernetes job.</p>
     *
     * @param yamlContentString a string of user-customized YAML string wrapped up in JSON
     * @return a {@link TaskResponse} object containing the result of the task execution.
     * @throws Exception if an error occurs during task execution or while handling pod logs.
     */
    @Override
    public TaskResponse run(String yamlContentString) throws Exception {
        TaskResponse result = new TaskResponse();
        int taskInstanceId = taskRequest.getTaskInstanceId();
        try {
            if (StringUtils.isEmpty(yamlContentString)) {
                return result;
            }

            K8sTaskExecutionContext k8sTaskExecutionContext = taskRequest.getK8sTaskExecutionContext();
            k8sUtils.buildClient(k8sTaskExecutionContext.getConfigYaml());

            // parse user-customized YAML string wrapped up in JSON
            // i.e., `K8sYamlContentDto` looks like: `{"type": ${K8sYamlType.POD}, "yaml": ${yaml} }`
            K8sYamlContentDTO k8sYamlContentDTO = JSONUtils.parseObject(yamlContentString, K8sYamlContentDTO.class);
            k8sYamlType = Objects.requireNonNull(k8sYamlContentDTO).getType();
            generateOperation();

            submitJob2k8s(yamlContentString);
            parseLogOutput();
            registerBatchK8sYamlTaskWatcher(String.valueOf(taskInstanceId), result);

            if (podLogOutputFuture != null) {
                try {
                    // Wait kubernetes pod log collection finished
                    podLogOutputFuture.get();
                } catch (ExecutionException e) {
                    log.error("Handle pod log error", e);
                }
            }
        } catch (Exception e) {
            cancelApplication(yamlContentString);
            Thread.currentThread().interrupt();
            result.setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
            throw e;
        }
        return result;
    }

    @Override
    public void cancelApplication(String k8sParameterStr) {
        if (metadata != null) {
            stopJobOnK8s(k8sParameterStr);
        }
    }

    @Override
    public void submitJob2k8s(String yamlContentString) {
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT);
        int taskInstanceId = taskRequest.getTaskInstanceId();
        String k8sJobName = String.format("%s-%s", taskName, taskInstanceId);

        K8sYamlContentDTO yamlContentDto =
                JSONUtils.parseObject(yamlContentString, K8sYamlContentDTO.class);

        metadata = abstractK8sOperation.buildMetadata(yamlContentDto);

        Map<String, String> labelMap = metadata.getMetadata().getLabels();
        if (MapUtils.isEmpty(labelMap)) {
            labelMap = new HashMap<String, String>(1);
        }

        try {
            log.info("[K8sYamlJobExecutor-{}-{}] start to submit job", taskName, taskInstanceId);
            abstractK8sOperation.createOrReplaceMetadata(metadata);
            log.info("[K8sYamlJobExecutor-{}-{}] submitted job successfully", taskName, taskInstanceId);
        } catch (Exception e) {
            log.error("[K8sYamlJobExecutor-{}-{}] fail to submit job", taskName, taskInstanceId);
            throw new TaskException("K8sYamlJobExecutor fail to submit job", e);
        }
    }

    @Override
    public void stopJobOnK8s(String k8sParameterStr) {
        String namespaceName = metadata.getMetadata().getNamespace();
        String jobName = metadata.getMetadata().getName();
        try {
            abstractK8sOperation.stopMetadata(metadata);
        } catch (Exception e) {
            log.error("[K8sYamlJobExecutor-{}] fail to stop job in namespace {}", jobName, namespaceName);
            throw new TaskException("K8sYamlJobExecutor fail to stop job", e);
        }
    }

    private void generateOperation() {
        switch (k8sYamlType) {
            case POD:
                abstractK8sOperation = new K8sPodOperation(k8sUtils.getClient());
            default:
                throw new TaskException(String.format("do not support type %s", k8sYamlType.name()));
        }
    }

    public void registerBatchK8sYamlTaskWatcher(String taskInstanceId, TaskResponse taskResponse) {
        CountDownLatch countDownLatch = new CountDownLatch(1);

        try (
                Watch watch =
                        abstractK8sOperation.createBatchWatcher(countDownLatch, taskResponse, metadata, taskRequest)) {
            boolean timeoutFlag = taskRequest.getTaskTimeoutStrategy() == TaskTimeoutStrategy.FAILED
                    || taskRequest.getTaskTimeoutStrategy() == TaskTimeoutStrategy.WARNFAILED;
            if (timeoutFlag) {
                Boolean timeout = !(countDownLatch.await(taskRequest.getTaskTimeout(), TimeUnit.SECONDS));
                waitTimeout(timeout);
            } else {
                countDownLatch.await();
            }
        } catch (InterruptedException e) {
            log.error("job failed in k8s: {}", e.getMessage(), e);
            Thread.currentThread().interrupt();
            taskResponse.setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
        } catch (Exception e) {
            log.error("job failed in k8s: {}", e.getMessage(), e);
            taskResponse.setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
        }
    }

    private void parseLogOutput() {
        ExecutorService collectPodLogExecutorService = ThreadUtils
                .newSingleDaemonScheduledExecutorService("CollectPodLogOutput-thread-" + taskRequest.getTaskName());

        String taskInstanceId = String.valueOf(taskRequest.getTaskInstanceId());
        String taskName = taskRequest.getTaskName().toLowerCase(Locale.ROOT);
        String containerName = String.format("%s-%s", taskName, taskInstanceId);
        podLogOutputFuture = collectPodLogExecutorService.submit(() -> {
            TaskOutputParameterParser taskOutputParameterParser = new TaskOutputParameterParser();
            LogUtils.setWorkflowAndTaskInstanceIDMDC(taskRequest.getProcessInstanceId(),
                    taskRequest.getTaskInstanceId());
            LogUtils.setTaskInstanceLogFullPathMDC(taskRequest.getLogPath());
            try (
                    LogWatch watcher =
                            abstractK8sOperation.getLogWatcher(containerName, metadata.getMetadata().getNamespace())) {
                String line;
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(watcher.getOutput()))) {
                    while ((line = reader.readLine()) != null) {
                        log.info("[K8S-pod-log] {}", line);
                        taskOutputParameterParser.appendParseLog(line);
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                LogUtils.removeTaskInstanceLogFullPathMDC();
                podLogOutputIsFinished = true;
            }
            taskOutputParams = taskOutputParameterParser.getTaskOutputParams();
        });

        collectPodLogExecutorService.shutdown();
    }
}
