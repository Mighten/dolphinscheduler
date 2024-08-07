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

import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;
import org.apache.dolphinscheduler.plugin.task.api.parameters.K8sYamlContentDTO;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.dsl.LogWatch;

public interface AbstractK8sOperation {

    int MAX_RETRY_TIMES = 3;

    HasMetadata buildMetadata(K8sYamlContentDTO yamlContentDto);

    /**
     * create or replace a resource in the kubernetes cluster
     * @param metadata resource metadata, e.g., io.fabric8.kubernetes.api.model.Pod
     * @throws Exception if error occurred in creating or replacing a resource
     */
    void createOrReplaceMetadata(HasMetadata metadata) throws Exception;

    /**
     * stop a resource in the kubernetes cluster
     * @param metadata resource metadata, e.g., io.fabric8.kubernetes.api.model.Pod
     * @return a list of StatusDetails
     * @throws Exception if error occurred in stopping a resource
     */
    List<StatusDetails> stopMetadata(HasMetadata metadata) throws Exception;

    int getState(HasMetadata hasMetadata);

    Watch createBatchWatcher(CountDownLatch countDownLatch,
                             TaskResponse taskResponse, HasMetadata hasMetadata,
                             TaskExecutionContext taskRequest);

    LogWatch getLogWatcher(String labelValue, String namespace);

    default void setTaskStatus(HasMetadata metadata, int jobStatus, String taskInstanceId, TaskResponse taskResponse) {
        if (jobStatus == TaskConstants.EXIT_CODE_SUCCESS || jobStatus == TaskConstants.EXIT_CODE_FAILURE) {
            if (jobStatus == TaskConstants.EXIT_CODE_SUCCESS) {
                // log.info("[K8sYamlJobExecutor-{}] succeed in k8s", metadata.getMetadata().getName());
                taskResponse.setExitStatusCode(TaskConstants.EXIT_CODE_SUCCESS);
            } else {
                // log.error("[K8sYamlJobExecutor-{}] fail in k8s", metadata.getMetadata().getName());
                taskResponse.setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
            }
        }
    }
}
