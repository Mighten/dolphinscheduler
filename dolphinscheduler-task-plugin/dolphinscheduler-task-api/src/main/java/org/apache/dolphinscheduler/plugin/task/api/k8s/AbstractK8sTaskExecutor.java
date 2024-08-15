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

import org.apache.dolphinscheduler.common.utils.ClassFilterConstructor;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.TaskResponse;
import org.apache.dolphinscheduler.plugin.task.api.utils.K8sUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.yaml.snakeyaml.Yaml;

public abstract class AbstractK8sTaskExecutor {

    protected TaskExecutionContext taskRequest;
    protected K8sUtils k8sUtils;
    protected Yaml yaml;
    protected volatile Map<String, String> taskOutputParams;
    protected AbstractK8sTaskExecutor(TaskExecutionContext taskRequest) {
        this.taskRequest = taskRequest;
        this.k8sUtils = new K8sUtils();
        this.yaml = new Yaml(new ClassFilterConstructor(new Class[]{
                List.class,
                String.class
        }));
        this.taskOutputParams = new HashMap<>();
    }
    public Map<String, String> getTaskOutputParams() {
        return taskOutputParams;
    }

    /**
     * Executes a task based on the provided Kubernetes parameters.
     *
     * <p>This method processes the input parameter which can either be a custom configuration
     * of type {@link K8sTaskMainParameters} or YAML content describing the Kubernetes job.</p>
     *
     * @param k8sParameterStr a string of either user-customized YAML string wrapped up in JSON or K8sTaskMainParameters::toString
     * @return a {@link TaskResponse} object containing the result of the task execution.
     * @throws Exception if an error occurs during task execution or while handling pod logs.
     */
    public abstract TaskResponse run(String k8sParameterStr) throws Exception;

    public abstract void cancelApplication(String k8sParameterStr);

    public void waitTimeout(Boolean timeout) throws TaskException {
        if (Boolean.TRUE.equals(timeout)) {
            throw new TaskException("K8sTask is timeout");
        }
    }

    public abstract void submitJob2k8s(String k8sParameterStr);

    public abstract void stopJobOnK8s(String k8sParameterStr);
}
