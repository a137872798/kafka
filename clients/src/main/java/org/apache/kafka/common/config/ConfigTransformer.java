/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.config;

import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.common.config.provider.FileConfigProvider;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class wraps a set of {@link ConfigProvider} instances and uses them to perform
 * transformations.
 *
 * <p>The default variable pattern is of the form <code>${provider:[path:]key}</code>,
 * where the <code>provider</code> corresponds to a {@link ConfigProvider} instance, as passed to
 * {@link ConfigTransformer#ConfigTransformer(Map)}.  The pattern will extract a set
 * of paths (which are optional) and keys and then pass them to {@link ConfigProvider#get(String, Set)} to obtain the
 * values with which to replace the variables.
 *
 * <p>For example, if a Map consisting of an entry with a provider name "file" and provider instance
 * {@link FileConfigProvider} is passed to the {@link ConfigTransformer#ConfigTransformer(Map)}, and a Properties
 * file with contents
 * <pre>
 * fileKey=someValue
 * </pre>
 * resides at the path "/tmp/properties.txt", then when a configuration Map which has an entry with a key "someKey" and
 * a value "${file:/tmp/properties.txt:fileKey}" is passed to the {@link #transform(Map)} method, then the transformed
 * Map will have an entry with key "someKey" and a value "someValue".
 *
 * <p>This class only depends on {@link ConfigProvider#get(String, Set)} and does not depend on subscription support
 * in a {@link ConfigProvider}, such as the {@link ConfigProvider#subscribe(String, Set, ConfigChangeCallback)} and
 * {@link ConfigProvider#unsubscribe(String, Set, ConfigChangeCallback)} methods.
 * 该对象负责协调provider与原始配置(map) 通过provider提供的值替换map中配置值的占位符
 */
public class ConfigTransformer {
    public static final Pattern DEFAULT_PATTERN = Pattern.compile("\\$\\{([^}]*?):(([^}]*?):)?([^}]*?)\\}");
    private static final String EMPTY_PATH = "";

    /**
     * 通过这组provider提供占位符的值
     */
    private final Map<String, ConfigProvider> configProviders;

    /**
     * Creates a ConfigTransformer with the default pattern, of the form <code>${provider:[path:]key}</code>.
     *
     * @param configProviders a Map of provider names and {@link ConfigProvider} instances.
     */
    public ConfigTransformer(Map<String, ConfigProvider> configProviders) {
        this.configProviders = configProviders;
    }

    /**
     * Transforms the given configuration data by using the {@link ConfigProvider} instances to
     * look up values to replace the variables in the pattern.
     *
     * @param configs the configuration values to be transformed
     * @return an instance of {@link ConfigTransformerResult}
     * 找到config中的占位符 并从这组provider对象中生成相关信息
     */
    public ConfigTransformerResult transform(Map<String, String> configs) {
        Map<String, Map<String, Set<String>>> keysByProvider = new HashMap<>();
        Map<String, Map<String, Map<String, String>>> lookupsByProvider = new HashMap<>();

        // Collect the variables from the given configs that need transformation
        for (Map.Entry<String, String> config : configs.entrySet()) {
            if (config.getValue() != null) {
                // 找到占位符
                List<ConfigVariable> vars = getVars(config.getValue(), DEFAULT_PATTERN);
                for (ConfigVariable var : vars) {
                    // 外层是providerName 内层是配置值path 最后是占位符
                    Map<String, Set<String>> keysByPath = keysByProvider.computeIfAbsent(var.providerName, k -> new HashMap<>());
                    Set<String> keys = keysByPath.computeIfAbsent(var.path, k -> new HashSet<>());
                    keys.add(var.variable);
                }
            }
        }

        // Retrieve requested variables from the ConfigProviders
        // 上面已经提取出了所有占位符 这里从provider中找到对应的值
        Map<String, Long> ttls = new HashMap<>();
        for (Map.Entry<String, Map<String, Set<String>>> entry : keysByProvider.entrySet()) {
            String providerName = entry.getKey();
            ConfigProvider provider = configProviders.get(providerName);
            Map<String, Set<String>> keysByPath = entry.getValue();
            if (provider != null && keysByPath != null) {
                for (Map.Entry<String, Set<String>> pathWithKeys : keysByPath.entrySet()) {
                    String path = pathWithKeys.getKey();
                    // 该path下所有占位符的key
                    Set<String> keys = new HashSet<>(pathWithKeys.getValue());
                    // 通过provider得到配置数据
                    ConfigData configData = provider.get(path, keys);
                    Map<String, String> data = configData.data();
                    Long ttl = configData.ttl();
                    if (ttl != null && ttl >= 0) {
                        ttls.put(path, ttl);
                    }
                    // 将最后的结果存储到该map中
                    Map<String, Map<String, String>> keyValuesByPath =
                            lookupsByProvider.computeIfAbsent(providerName, k -> new HashMap<>());
                    keyValuesByPath.put(path, data);
                }
            }
        }

        // Perform the transformations by performing variable replacements
        Map<String, String> data = new HashMap<>(configs);
        for (Map.Entry<String, String> config : configs.entrySet()) {
            // 使用替换后的数据覆盖旧数据
            data.put(config.getKey(), replace(lookupsByProvider, config.getValue(), DEFAULT_PATTERN));
        }
        return new ConfigTransformerResult(data, ttls);
    }

    private static List<ConfigVariable> getVars(String value, Pattern pattern) {
        List<ConfigVariable> configVars = new ArrayList<>();
        Matcher matcher = pattern.matcher(value);
        while (matcher.find()) {
            configVars.add(new ConfigVariable(matcher));
        }
        return configVars;
    }

    private static String replace(Map<String, Map<String, Map<String, String>>> lookupsByProvider,
                                  String value,
                                  Pattern pattern) {
        if (value == null) {
            return null;
        }
        Matcher matcher = pattern.matcher(value);
        StringBuilder builder = new StringBuilder();
        int i = 0;
        while (matcher.find()) {
            ConfigVariable configVar = new ConfigVariable(matcher);
            Map<String, Map<String, String>> lookupsByPath = lookupsByProvider.get(configVar.providerName);
            if (lookupsByPath != null) {
                Map<String, String> keyValues = lookupsByPath.get(configVar.path);
                String replacement = keyValues.get(configVar.variable);
                builder.append(value, i, matcher.start());
                if (replacement == null) {
                    // No replacements will be performed; just return the original value
                    builder.append(matcher.group(0));
                } else {
                    builder.append(replacement);
                }
                i = matcher.end();
            }
        }
        builder.append(value, i, value.length());
        return builder.toString();
    }

    /**
     * 当匹配到某个占位符时 生成该对象
     */
    private static class ConfigVariable {
        /**
         * 代表该占位符的数据需要从哪个provider上获取
         */
        final String providerName;
        final String path;
        final String variable;

        ConfigVariable(Matcher matcher) {
            this.providerName = matcher.group(1);
            this.path = matcher.group(3) != null ? matcher.group(3) : EMPTY_PATH;
            this.variable = matcher.group(4);
        }

        public String toString() {
            return "(" + providerName + ":" + (path != null ? path + ":" : "") + variable + ")";
        }
    }
}
