/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.skywalking.apm.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Utility class for working with Strings that have placeholder values in them. A placeholder takes the form {@code
 * ${name}}. Using {@code PropertyPlaceholderHelper} these placeholders can be substituted for user-supplied values. <p>
 * Values for substitution can be supplied using a {@link Properties} instance or using a {@link PlaceholderResolver}.
 * 占位符替换对象
 */
public enum PropertyPlaceholderHelper {

    INSTANCE(PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_PREFIX, PlaceholderConfigurerSupport.DEFAULT_PLACEHOLDER_SUFFIX, PlaceholderConfigurerSupport.DEFAULT_VALUE_SEPARATOR, true);

    private final String placeholderPrefix;

    private final String placeholderSuffix;

    private final String simplePrefix;

    private final String valueSeparator;

    private final boolean ignoreUnresolvablePlaceholders;

    /**
     * Creates a new {@code PropertyPlaceholderHelper} that uses the supplied prefix and suffix.
     *
     * @param placeholderPrefix              the prefix that denotes the start of a placeholder
     * @param placeholderSuffix              the suffix that denotes the end of a placeholder
     * @param valueSeparator                 the separating character between the placeholder variable and the
     *                                       associated default value, if any
     * @param ignoreUnresolvablePlaceholders indicates whether unresolvable placeholders should be ignored ({@code
     *                                       true}) or cause an exception ({@code false})
     */
    PropertyPlaceholderHelper(String placeholderPrefix, String placeholderSuffix, String valueSeparator,
                              boolean ignoreUnresolvablePlaceholders) {
        if (StringUtil.isEmpty(placeholderPrefix) || StringUtil.isEmpty(placeholderSuffix)) {
            throw new UnsupportedOperationException("'placeholderPrefix or placeholderSuffix' must not be null");
        }

        final Map<String, String> wellKnownSimplePrefixes = new HashMap<String, String>(4);

        wellKnownSimplePrefixes.put("}", "{");
        wellKnownSimplePrefixes.put("]", "[");
        wellKnownSimplePrefixes.put(")", "(");

        this.placeholderPrefix = placeholderPrefix;
        this.placeholderSuffix = placeholderSuffix;
        String simplePrefixForSuffix = wellKnownSimplePrefixes.get(this.placeholderSuffix);
        if (simplePrefixForSuffix != null && this.placeholderPrefix.endsWith(simplePrefixForSuffix)) {
            this.simplePrefix = simplePrefixForSuffix;
        } else {
            this.simplePrefix = this.placeholderPrefix;
        }
        this.valueSeparator = valueSeparator;
        this.ignoreUnresolvablePlaceholders = ignoreUnresolvablePlaceholders;
    }

    /**
     * Replaces all placeholders of format {@code ${name}} with the corresponding property from the supplied {@link
     * Properties}.
     *
     * @param value      the value containing the placeholders to be replaced
     * @param properties the {@code Properties} to use for replacement
     * @return the supplied value with placeholders replaced inline
     */
    public String replacePlaceholders(String value, final Properties properties) {
        return replacePlaceholders(value, new PlaceholderResolver() {
            @Override
            public String resolvePlaceholder(String placeholderName) {
                return PropertyPlaceholderHelper.this.getConfigValue(placeholderName, properties);
            }
        });
    }

    private String getConfigValue(String key, final Properties properties) {
        // 从这里可以看出 系统变量 和环境变量的优先级 高于配置
        String value = System.getProperty(key);
        if (StringUtil.isEmpty(value)) {
            value = System.getenv(key);
        }
        if (StringUtil.isEmpty(value)) {
            value = properties.getProperty(key);
        }
        return value;
    }

    /**
     * Replaces all placeholders of format {@code ${name}} with the value returned from the supplied {@link
     * PlaceholderResolver}.
     *
     * @param value               the value containing the placeholders to be replaced     包含了占位符的原始字符
     * @param placeholderResolver the {@code PlaceholderResolver} to use for replacement   该对象包含了替换占位符的逻辑 比如从某个prop 中获取对应属性
     * @return the supplied value with placeholders replaced inline
     */
    public String replacePlaceholders(String value, PlaceholderResolver placeholderResolver) {
        return parseStringValue(value, placeholderResolver, new HashSet<String>());
    }

    /**
     * @param value
     * @param placeholderResolver
     * @param visitedPlaceholders
     * @return 返回的是已经处理完占位符的结果字符串
     */
    protected String parseStringValue(String value, PlaceholderResolver placeholderResolver,
                                      Set<String> visitedPlaceholders) {

        StringBuilder result = new StringBuilder(value);

        // 首先寻找到 占位符前缀
        int startIndex = value.indexOf(this.placeholderPrefix);
        while (startIndex != -1) {
            // 找到 ${ 对应的 } 的下标 包含了处理嵌套的逻辑
            int endIndex = findPlaceholderEndIndex(result, startIndex);
            if (endIndex != -1) {
                // 截取占位符内部的内容 如果发现嵌套的情况
                String placeholder = result.substring(startIndex + this.placeholderPrefix.length(), endIndex);
                String originalPlaceholder = placeholder;
                // 这里是判断是否发生循环依赖
                if (!visitedPlaceholders.add(originalPlaceholder)) {
                    throw new IllegalArgumentException("Circular placeholder reference '" + originalPlaceholder + "' in property definitions");
                }
                // Recursive invocation, parsing placeholders contained in the placeholder key.
                // 这里是递归处理 嵌套的情况  也就是该方法最终返回的字符串不再嵌套了
                placeholder = parseStringValue(placeholder, placeholderResolver, visitedPlaceholders);
                // Now obtain the value for the fully resolved key...
                // 从备选的prop 找到占位符对应的值 并进行替换
                String propVal = placeholderResolver.resolvePlaceholder(placeholder);
                // 如果为空的话 要考虑是否设置了默认值 从这里可以看出 skywalking 认为 设置默认值的情况是少数 所有只有直接替换失败时才解析默认值
                if (propVal == null && this.valueSeparator != null) {
                    int separatorIndex = placeholder.indexOf(this.valueSeparator);
                    if (separatorIndex != -1) {
                        String actualPlaceholder = placeholder.substring(0, separatorIndex);
                        String defaultValue = placeholder.substring(separatorIndex + this.valueSeparator.length());
                        // 还是没有找到的话 就设置成默认值
                        propVal = placeholderResolver.resolvePlaceholder(actualPlaceholder);
                        if (propVal == null) {
                            propVal = defaultValue;
                        }
                    }
                }
                if (propVal != null) {
                    // Recursive invocation, parsing placeholders contained in the
                    // previously resolved placeholder value.
                    // 替换的结果 还可能存在占位符 所以要递归调用 同时 通过visitedPlaceholders 进行循环依赖检测
                    propVal = parseStringValue(propVal, placeholderResolver, visitedPlaceholders);
                    // 这里返回的字符串一定是不包含占位符的最终结果了
                    result.replace(startIndex, endIndex + this.placeholderSuffix.length(), propVal);
                    startIndex = result.indexOf(this.placeholderPrefix, startIndex + propVal.length());
                } else if (this.ignoreUnresolvablePlaceholders) {
                    // Proceed with unprocessed value.
                    startIndex = result.indexOf(this.placeholderPrefix, endIndex + this.placeholderSuffix.length());
                } else {
                    throw new IllegalArgumentException("Could not resolve placeholder '" + placeholder + "'" + " in value \"" + value + "\"");
                }
                visitedPlaceholders.remove(originalPlaceholder);
            } else {
                startIndex = -1;
            }
        }
        return result.toString();
    }

    /**
     * 找到与当前占位符 开口对应的 结尾位置  (这里还要关注嵌套的情况)
     *
     * @param buf
     * @param startIndex
     * @return
     */
    private int findPlaceholderEndIndex(CharSequence buf, int startIndex) {
        int index = startIndex + this.placeholderPrefix.length();
        int withinNestedPlaceholder = 0;
        while (index < buf.length()) {
            // 判断下一个字符 是否匹配上  }
            if (StringUtil.substringMatch(buf, index, this.placeholderSuffix)) {
                // 如果出现了嵌套的情况 那么先减少嵌套层数
                if (withinNestedPlaceholder > 0) {
                    withinNestedPlaceholder--;
                    index = index + this.placeholderSuffix.length();
                } else {
                    // 代表找到了当前 ${ 对应的 } 的下标
                    return index;
                }
                // 如果有发现了前缀 代表发生了嵌套
            } else if (StringUtil.substringMatch(buf, index, this.simplePrefix)) {
                // 增加嵌套层数
                withinNestedPlaceholder++;
                index = index + this.simplePrefix.length();
            } else {
                // 移动光标准备匹配下一个char
                index++;
            }
        }
        return -1;
    }

    /**
     * Strategy interface used to resolve replacement values for placeholders contained in Strings.
     */
    public interface PlaceholderResolver {

        /**
         * Resolve the supplied placeholder name to the replacement value.
         *
         * @param placeholderName the name of the placeholder to resolve
         * @return the replacement value, or {@code null} if no replacement is to be made
         */
        String resolvePlaceholder(String placeholderName);
    }
}
