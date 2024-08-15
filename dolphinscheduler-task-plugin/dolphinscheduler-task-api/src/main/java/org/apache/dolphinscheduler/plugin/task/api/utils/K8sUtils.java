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

package org.apache.dolphinscheduler.plugin.task.api.utils;

import org.apache.dolphinscheduler.common.utils.YamlUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskConstants;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import com.fasterxml.jackson.core.type.TypeReference;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.api.model.StatusDetails;
import io.fabric8.kubernetes.api.model.batch.v1.Job;
import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientBuilder;
import io.fabric8.kubernetes.client.Watch;
import io.fabric8.kubernetes.client.Watcher;

@Slf4j
@Data
public class K8sUtils {

    private KubernetesClient client;

    private static final String K8S_NAMESPACE_DEFAULT = "default";

    /**
     * get the original namespace, or default namespace defined in `K8sUtils.K8S_NAMESPACE_DEFAULT`
     * @param namespace the namespace to be inspected
     * @return the original namespace if it is valid; otherwise, return default namespace
     */
    public static String getOrDefaultNamespace(String namespace) {
        return StringUtils.isBlank(namespace) ? K8S_NAMESPACE_DEFAULT : namespace;
    }

    /**
     * get the original resource, or the one with default namespace
     * @param resource the resource to be inspected
     * @return the original resource, or the default namespaced one
     * @throws TaskException if resource is null
     */
    public static HasMetadata getOrDefaultNamespacedResource(HasMetadata resource) throws TaskException {
        if (resource == null)
            throw new TaskException("failed to process k8s resource with null parameter");
        ObjectMeta metadata = resource.getMetadata();
        if (StringUtils.isBlank(metadata.getNamespace())) {
            metadata.setNamespace(K8S_NAMESPACE_DEFAULT);
            resource.setMetadata(metadata);
        }
        return resource;
    }

    /**
     * create a Config Map from YAML file
     *
     * @param configMapFile the YAML file to load
     * @return the Config Map created
     */
    public ConfigMap createConfigMap(File configMapFile) {
        try {
            ConfigMap configMap = (ConfigMap) getOrDefaultNamespacedResource(
                    YamlUtils.load(configMapFile, new TypeReference<ConfigMap>() {
                    }));
            return client.configMaps().resource(configMap).create();
        } catch (Exception e) {
            throw new TaskException("fail to create ConfigMap", e);
        }
    }

    /**
     * create pod from YAML file
     *
     * @param podFile the YAML file to load
     * @return the pod created
     */
    public Pod createPod(File podFile) {
        try {
            Pod pod = (Pod) getOrDefaultNamespacedResource(
                    YamlUtils.load(podFile, new TypeReference<Pod>() {
                    }));
            return client.pods().resource(pod).create();
        } catch (Exception e) {
            throw new TaskException("fail to create pod", e);
        }
    }

    /**
     * delete pod in the namespace
     *
     * @param namespace the namespace of the Pod
     * @param podName the pod name
     * @return a list of StatusDetails
     */
    public List<StatusDetails> deletePod(String namespace, String podName) {
        try {
            namespace = getOrDefaultNamespace(namespace);
            return client.pods()
                    .inNamespace(namespace)
                    .withName(podName)
                    .delete();
        } catch (Exception e) {
            String errorMessage = String.format("fail to delete pod '%s' in Namespace '%s'", podName, namespace);
            throw new TaskException(errorMessage, e);
        }
    }

    /**
     * delete Config Map in the namespace
     *
     * @param namespace the namespace of the Config Map
     * @param configMapName the name of the Config Map
     * @return a list of StatusDetails
     */
    public List<StatusDetails> deleteConfigMap(String namespace, String configMapName) {
        try {
            namespace = getOrDefaultNamespace(namespace);
            return client.configMaps()
                    .inNamespace(namespace)
                    .withName(configMapName)
                    .delete();
        } catch (Exception e) {
            String errorMessage =
                    String.format("fail to delete ConfigMap '%s' in Namespace '%s'", configMapName, namespace);
            throw new TaskException(errorMessage, e);
        }
    }

    public void createJob(String namespace, Job job) {
        try {
            job = (Job) getOrDefaultNamespacedResource(job);
            client.batch()
                    .v1()
                    .jobs()
                    .resource(job)
                    .create();
        } catch (Exception e) {
            throw new TaskException("fail to create job", e);
        }
    }

    public void deleteJob(String jobName, String namespace) {
        try {
            client.batch()
                    .v1()
                    .jobs()
                    .inNamespace(getOrDefaultNamespace(namespace))
                    .withName(jobName)
                    .delete();
        } catch (Exception e) {
            throw new TaskException("fail to delete job", e);
        }
    }

    public Boolean jobExist(String jobName, String namespace) {
        try {
            Job job = client.batch().v1().jobs().inNamespace(namespace).withName(jobName).get();
            return job != null;
        } catch (Exception e) {
            throw new TaskException("fail to check job: ", e);
        }
    }

    public Watch createBatchJobWatcher(String jobName, Watcher<Job> watcher) {
        try {
            return client.batch()
                    .v1()
                    .jobs()
                    .withName(jobName)
                    .watch(watcher);
        } catch (Exception e) {
            throw new TaskException("fail to register batch job watcher", e);
        }
    }

    /**
     * Print Log from a namespaced pod
     *
     * @param namespace the name of Namespace
     * @return Pod logs of Pretty output
     */
    public List<String> getLogsOfNamespacedPods(String namespace) {
        namespace = getOrDefaultNamespace(namespace);
        final String namespaceFinalInStreamOperation = namespace;
        try {
            List<Pod> podList = client.pods().inNamespace(namespace).list().getItems();
            return podList.stream()
                    .map((Pod pod) -> {
                        String podName = pod.getMetadata().getName();
                        return client.pods()
                                .inNamespace(namespaceFinalInStreamOperation)
                                .withName(podName)
                                .tailingLines(TaskConstants.LOG_LINES)
                                .getLog(Boolean.TRUE);
                    })
                    .collect(Collectors.toList());
        } catch (Exception e) {
            log.error("fail to getPodLog", e);
            log.error("response bodies : {}", e.getMessage());
            return null;
        }
    }

    public String getPodLog(String jobName, String namespace) {
        try {
            namespace = getOrDefaultNamespace(namespace);
            List<Pod> podList = client.pods().inNamespace(namespace).list().getItems();
            String podName = null;
            for (Pod pod : podList) {
                podName = pod.getMetadata().getName();
                if (podName.contains("-") && jobName.equals(podName.substring(0, podName.lastIndexOf("-")))) {
                    break;
                }
            }
            return client.pods().inNamespace(namespace)
                    .withName(podName)
                    .tailingLines(TaskConstants.LOG_LINES)
                    .getLog(Boolean.TRUE);
        } catch (Exception e) {
            log.error("fail to getPodLog", e);
            log.error("response bodies : {}", e.getMessage());
        }
        return null;
    }

    /**
     * Builds a Kubernetes API client using a kubeConfig YAML string.
     *
     * @param configYaml a YAML string containing the Kubernetes configuration
     * @throws TaskException if there is an error building the Kubernetes client
     */
    public void buildClient(String configYaml) throws TaskException {
        try {
            Config config = Config.fromKubeconfig(configYaml);
            client = new KubernetesClientBuilder().withConfig(config).build();
        } catch (Exception e) {
            throw new TaskException("fail to build k8s ApiClient", e);
        }
    }

    public KubernetesClient getClient() {
        return client;
    }

}
