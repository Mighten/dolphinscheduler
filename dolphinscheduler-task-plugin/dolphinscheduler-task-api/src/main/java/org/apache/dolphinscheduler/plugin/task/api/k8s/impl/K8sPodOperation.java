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

package org.apache.dolphinscheduler.plugin.task.api.k8s.impl;

import org.apache.dolphinscheduler.common.thread.ThreadUtils;
import org.apache.dolphinscheduler.common.utils.YamlUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.enums.K8sPodPhaseConstants;
import org.apache.dolphinscheduler.plugin.task.api.k8s.AbstractK8sOperation;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;
import org.apache.dolphinscheduler.plugin.task.api.parameters.K8sYamlContentDto;
import org.apache.dolphinscheduler.plugin.task.api.utils.K8sUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;

import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.core.type.TypeReference;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.PodList;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;
import io.fabric8.kubernetes.client.WatcherException;
import io.fabric8.kubernetes.client.dsl.FilterWatchListDeletable;
import io.fabric8.kubernetes.client.dsl.LogWatch;
import io.fabric8.kubernetes.client.dsl.PodResource;

@Slf4j
public class K8sPodOperation implements AbstractK8sOperation {

    private KubernetesClient client;

    public K8sPodOperation(KubernetesClient client) {
        this.client = client;
    }

    @Override
    public HasMetadata buildMetadata(K8sYamlContentDto yamlContentDto) {
        String yamlK8sResourceStr = yamlContentDto.getYaml();
        Pod pod = (Pod) K8sUtils.getOrDefaultNamespacedResource(
                YamlUtils.load(yamlK8sResourceStr, new TypeReference<Pod>() {
                }));
        return client.pods().resource(pod).get();
    }

    /**
     * create or replace a pod in the kubernetes cluster
     * @param metadata Pod metadata (io.fabric8.kubernetes.api.model.Pod)
     * @throws Exception if error occurred in creating or replacing a resource
     */
    @Override
    public void createOrReplaceMetadata(HasMetadata metadata) throws Exception {
        Pod pod = (Pod) K8sUtils.getOrDefaultNamespacedResource(metadata);
        client.pods().resource(pod).createOrReplace();
    }

    @Override
    public int getState(HasMetadata hasMetadata) {
        Pod pod = (Pod) K8sUtils.getOrDefaultNamespacedResource(hasMetadata);
        String currentPodPhase = pod.getStatus().getPhase();

        if (K8sPodPhaseConstants.SUCCEEDED.equals(currentPodPhase)) {
            return TaskConstants.EXIT_CODE_SUCCESS;
        } else if (K8sPodPhaseConstants.FAILED.equals(currentPodPhase)) {
            return TaskConstants.EXIT_CODE_FAILURE;
        } else {
            return TaskConstants.RUNNING_CODE;
        }
    }

    @Override
    public Watch createBatchWatcher(CountDownLatch countDownLatch,
                                    TaskResponse taskResponse, HasMetadata hasMetadata,
                                    TaskExecutionContext taskRequest) {
        Watcher<Pod> watcher = new Watcher<Pod>() {

            @Override
            public void eventReceived(Action action, Pod pod) {
                try {
                    LogUtils.setWorkflowAndTaskInstanceIDMDC(taskRequest.getProcessInstanceId(),
                            taskRequest.getTaskInstanceId());
                    LogUtils.setTaskInstanceLogFullPathMDC(taskRequest.getLogPath());
                    log.info("event received : job:{} action:{}", pod.getMetadata().getName(), action);
                    if (action == Action.DELETED) {
                        log.error("[K8sJobExecutor-{}] fail in k8s", pod.getMetadata().getName());
                        taskResponse.setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
                        countDownLatch.countDown();
                    } else if (action != Action.ADDED) {
                        int jobStatus = getState(pod);
                        log.info("job {} status {}", pod.getMetadata().getName(), jobStatus);
                        if (jobStatus == TaskConstants.RUNNING_CODE) {
                            return;
                        }
                        setTaskStatus(hasMetadata, jobStatus, String.valueOf(taskRequest.getTaskInstanceId()),
                                taskResponse);
                        countDownLatch.countDown();
                    }
                } finally {
                    LogUtils.removeTaskInstanceLogFullPathMDC();
                    LogUtils.removeWorkflowAndTaskInstanceIdMDC();
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LogUtils.setWorkflowAndTaskInstanceIDMDC(taskRequest.getProcessInstanceId(),
                        taskRequest.getTaskInstanceId());
                log.error("[K8sJobExecutor-{}] fail in k8s: {}", hasMetadata.getMetadata().getName(), e.getMessage());
                taskResponse.setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
                countDownLatch.countDown();
                LogUtils.removeWorkflowAndTaskInstanceIdMDC();
            }
        };
        return client.pods().inNamespace(hasMetadata.getMetadata().getNamespace())
                .withName(hasMetadata.getMetadata().getName())
                .watch(watcher);
    }

    @Override
    public LogWatch getLogWatcher(String labelValue, String namespace) {
        namespace = K8sUtils.getOrDefaultNamespace(namespace);
        boolean metadataIsReady = false;
        Pod pod = null;
        while (!metadataIsReady) {
            FilterWatchListDeletable<Pod, PodList, PodResource> watchList =
                    getListenPod(labelValue, namespace);
            List<Pod> podList = watchList == null ? null : watchList.list().getItems();
            if (CollectionUtils.isEmpty(podList)) {
                return null;
            }
            pod = podList.get(0);
            String phase = pod.getStatus().getPhase();
            if (phase.equals(K8sPodPhaseConstants.PENDING) || phase.equals(K8sPodPhaseConstants.UNKNOWN)) {
                ThreadUtils.sleep(TaskConstants.SLEEP_TIME_MILLIS);
            } else {
                metadataIsReady = true;
            }
        }
        return client.pods().inNamespace(pod.getMetadata().getNamespace())
                .withName(pod.getMetadata().getName())
                .inContainer(pod.getMetadata().getName())
                .watchLog();
    }

    /**
     * stop a pod in the kubernetes cluster
     * @param metadata Pod metadata (io.fabric8.kubernetes.api.model.Pod)
     * @return a list of StatusDetails
     * @throws Exception if error occurred in stopping a resource
     */
    @Override
    public List<StatusDetails> stopMetadata(HasMetadata metadata) throws Exception {
        Pod pod = (Pod) K8sUtils.getOrDefaultNamespacedResource(metadata);
        return client.pods().resource(pod).delete();
    }

    /*
     * get driver pod
     */
    private FilterWatchListDeletable<Pod, PodList, PodResource> getListenPod(String labelValue, String namespace) {
        namespace = K8sUtils.getOrDefaultNamespace(namespace);
        List<Pod> podList = null;
        FilterWatchListDeletable<Pod, PodList, PodResource> watchList = null;
        int retryTimes = 0;
        while (CollectionUtils.isEmpty(podList) && retryTimes < AbstractK8sOperation.MAX_RETRY_TIMES) {
            watchList = client.pods()
                    .inNamespace(namespace)
                    .withLabel(TaskConstants.UNIQUE_LABEL_NAME, labelValue);
            podList = watchList.list().getItems();
            if (!CollectionUtils.isEmpty(podList)) {
                break;
            }
            ThreadUtils.sleep(TaskConstants.SLEEP_TIME_MILLIS);
            retryTimes += 1;
        }

        return watchList;
    }

}
