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
import org.apache.dolphinscheduler.plugin.task.api.k8s.K8sYamlTaskExecutor;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;
import org.apache.dolphinscheduler.plugin.task.api.utils.K8sUtils;
import org.apache.dolphinscheduler.plugin.task.api.utils.LogUtils;

import org.apache.commons.collections4.CollectionUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.core.type.TypeReference;

import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
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
    public HasMetadata buildMetadata(String yamlContentStr) {
        Pod pod = (Pod) K8sUtils.getOrDefaultNamespacedResource(
                YamlUtils.load(yamlContentStr, new TypeReference<Pod>() {
                }));
        return client.pods().resource(pod).get();
    }

    /**
     * create or replace a pod in the kubernetes cluster
     * @param resource Pod (io.fabric8.kubernetes.api.model.Pod)
     * @param taskInstanceId task instance id
     * @throws Exception if error occurred in creating or replacing a resource
     */
    @Override
    public void createOrReplaceMetadata(HasMetadata resource, int taskInstanceId) throws Exception {
        synchronized (K8sUtils.class) {
            log.info("[k8s-label-{}-{}] Enter createOrReplacePod for namespace `{}`",
                    resource.getMetadata().getName(), taskInstanceId, resource.getMetadata().getNamespace());
            Pod pod = (Pod) K8sUtils.getOrDefaultNamespacedResource(resource);
            ObjectMeta podMetadata = pod.getMetadata();
            if (client
                    .pods()
                    .inNamespace(podMetadata.getNamespace())
                    .withName(podMetadata.getName())
                    .get() != null) {
                stopMetadata(pod);
            }

            Map<String, String> labelPodLogWatchMap = new HashMap<>();
            labelPodLogWatchMap.put(K8sYamlTaskExecutor.DS_LOG_WATCH_LABEL_NAME,
                    String.format("%s-%d", podMetadata.getName(), taskInstanceId));
            podMetadata.setLabels(labelPodLogWatchMap);
            client.pods().resource(pod).createOrReplace();
            log.info("[k8s-label-{}-{}] Leave createOrReplacePod for namespace `{}`",
                    resource.getMetadata().getName(), taskInstanceId, resource.getMetadata().getNamespace());
        }
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
        final int taskInstanceId = taskRequest.getTaskInstanceId();
        final int processInstanceId = taskRequest.getProcessInstanceId();

        Watcher<Pod> watcher = new Watcher<Pod>() {

            @Override
            public void eventReceived(Action action, Pod pod) {
                try {
                    ObjectMeta podMetadata = pod.getMetadata();
                    LogUtils.setWorkflowAndTaskInstanceIDMDC(processInstanceId, taskInstanceId);
                    LogUtils.setTaskInstanceLogFullPathMDC(taskRequest.getLogPath());
                    log.info("[k8s-label-{}-{}] event received: action: {}", podMetadata.getName(), taskInstanceId,
                            action);
                    if (action == Action.DELETED) {
                        log.info("[k8s-label-{}-{}] to be deleted in k8s", podMetadata.getName(), taskInstanceId);
                        taskResponse.setExitStatusCode(TaskConstants.EXIT_CODE_FAILURE);
                        countDownLatch.countDown();
                    } else if (action != Action.ADDED) {
                        int jobStatus = getState(pod);
                        log.info("[k8s-label-{}-{}] status {}", podMetadata.getName(), taskInstanceId, jobStatus);
                        if (jobStatus == TaskConstants.RUNNING_CODE) {
                            return;
                        }
                        setTaskStatus(hasMetadata, jobStatus, String.valueOf(taskInstanceId), taskResponse);
                        countDownLatch.countDown();
                    }
                } finally {
                    LogUtils.removeTaskInstanceLogFullPathMDC();
                    LogUtils.removeWorkflowAndTaskInstanceIdMDC();
                }
            }

            @Override
            public void onClose(WatcherException e) {
                LogUtils.setWorkflowAndTaskInstanceIDMDC(processInstanceId, taskInstanceId);
                log.error("[k8s-label-{}-{}] fail in k8s: {}", hasMetadata.getMetadata().getName(), taskInstanceId,
                        e.getMessage());
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
                log.warn("[k8s-label-{}] no pod found in namespace `{}`", labelValue, namespace);
                return null;
            }
            log.info("[k8s-label-{}] found {} pod(s) in namespace `{}`", labelValue, podList.size(), namespace);
            pod = podList.get(0);
            String phase = pod.getStatus().getPhase();
            if (phase.equals(K8sPodPhaseConstants.PENDING) || phase.equals(K8sPodPhaseConstants.UNKNOWN)) {
                log.info("[k8s-label-{}] Pod `{}` in namespace `{}` is NOT Ready (Phase = {}), retry in {}ms",
                        labelValue, pod.getMetadata().getName(), namespace, phase, TaskConstants.SLEEP_TIME_MILLIS);
                ThreadUtils.sleep(TaskConstants.SLEEP_TIME_MILLIS);
            } else {
                log.info("[k8s-label-{}] Pod `{}` in namespace `{}` is Ready (Phase = {})",
                        labelValue, pod.getMetadata().getName(), namespace, phase);
                metadataIsReady = true;
            }
        }
        return client.pods().inNamespace(pod.getMetadata().getNamespace())
                .withName(pod.getMetadata().getName())
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
        String taskName = pod.getMetadata().getName();
        String namespace = pod.getMetadata().getNamespace();
        return client.pods().inNamespace(namespace).withName(taskName).delete();
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
                    .withLabel(K8sYamlTaskExecutor.DS_LOG_WATCH_LABEL_NAME, labelValue);
            podList = watchList.list().getItems();
            if (!CollectionUtils.isEmpty(podList)) {
                log.info("[k8s-label-{}] driver pod retrieved", labelValue);
                break;
            }
            log.info("[k8s-label-{}] Failed to get driver pod, retry in {}ms",
                    labelValue, TaskConstants.SLEEP_TIME_MILLIS);
            ThreadUtils.sleep(TaskConstants.SLEEP_TIME_MILLIS);
            retryTimes += 1;
        }

        return watchList;
    }

}
