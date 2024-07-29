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

package org.apache.dolphinscheduler.common.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import lombok.extern.slf4j.Slf4j;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.core.type.TypeReference;

@Slf4j
public class YamlUtilsTest {

    private static final String yamlStringExpected =
            "name: Yaml Parser" + '\n' +
                    "age: 30" + '\n' +
                    "address:" + '\n' +
                    "  city: New York" + '\n' +
                    "  state: NY" + '\n' +
                    "  zipcode: 10001" + '\n';

    private static final Map<String, Object> yamlDataMapExpected = new HashMap<String, Object>() {

        {
            put("name", "Yaml Parser");
            put("age", 30);
            put("address", new HashMap<String, Object>() {

                {
                    put("city", "New York");
                    put("state", "NY");
                    put("zipcode", 10001);
                }
            });
        }
    };

    @Test
    public void testParseYamlString() {

        log.debug("[*] ENTER Unit test for YamlUtils::parseYamlString");
        Map<String, Object> yamlDataMapActual =
                YamlUtils.load(yamlStringExpected, new TypeReference<Map<String, Object>>() {
                });
        Assertions.assertEquals(
                yamlDataMapExpected, yamlDataMapActual,
                "[!] Test FAILED: expected YAML data: " + yamlDataMapExpected +
                        ", but actual data parsed: " + yamlDataMapActual);
        log.debug("[+]     PASSED All Testcases: YamlUtils::parseYamlString");
        log.debug("[*] LEAVE Unit test for YamlUtils::parseYamlString");
    }

    @Test
    public void testParseYamlFile() {
        log.debug("[*] ENTER Unit test for YamlUtils::parseYamlFile");
        isFileTestcase01AddressYamlPassed();
        isFileTestcase02SimpleK8sPodYamlPassed();
        log.debug("[+]     PASSED All Testcases: YamlUtils::parseYamlFile");
        log.debug("[*] LEAVE Unit test for YamlUtils::parseYamlFile");
    }

    /*
     * The following methods are helpers and should be kept private
     */

    private void isFileTestcase01AddressYamlPassed() {
        String filePathRelative = "yaml/testcase-01-address.yaml";
        String filePathAbsolute =
                Objects.requireNonNull(getClass().getClassLoader().getResource(filePathRelative)).getFile();
        File file = new File(filePathAbsolute);
        Map<String, Object> yamlDataMapActual = YamlUtils.load(file, new TypeReference<Map<String, Object>>() {
        });
        Assertions.assertEquals(
                yamlDataMapExpected, yamlDataMapActual,
                "[!] Test FAILED on YAML file: yaml/testcase-01-address.yaml");
    }

    private void isFileTestcase02SimpleK8sPodYamlPassed() {
        String filePathRelative = "yaml/testcase-02-simple-k8s-pod.yaml";
        String filePathAbsolute =
                Objects.requireNonNull(getClass().getClassLoader().getResource(filePathRelative)).getFile();

        Map<String, Object> yamlDataK8sPodExpected = new HashMap<String, Object>() {

            {
                put("apiVersion", "v1");
                put("kind", "Pod");
                put("metadata", new HashMap<String, Object>() {

                    {
                        put("name", "testcase-02-simple-k8s-pod-nginx");
                        put("labels", new HashMap<String, Object>() {

                            {
                                put("app", "nginx");
                            }
                        });
                    }
                });
                put("spec", new HashMap<String, Object>() {

                    {
                        put("containers", new ArrayList<Object>() {

                            {
                                add(new HashMap<String, Object>() {

                                    {
                                        put("name", "nginx-container");
                                        put("image", "nginx:1.10");
                                        put("ports", new ArrayList<Object>() {

                                            {
                                                add(new HashMap<String, Object>() {

                                                    {
                                                        put("containerPort", 80);
                                                    }
                                                });
                                            }
                                        });
                                    }
                                });
                            }
                        });
                    }
                });
            }
        };
        File file = new File(filePathAbsolute);
        Map<String, Object> yamlDataK8sPodActual = YamlUtils.load(file, new TypeReference<Map<String, Object>>() {
        });
        Assertions.assertEquals(
                yamlDataK8sPodExpected, yamlDataK8sPodActual,
                "[!] Test FAILED on YAML file: yaml/testcase-02-simple-k8s-pod.yaml");
    }
}
