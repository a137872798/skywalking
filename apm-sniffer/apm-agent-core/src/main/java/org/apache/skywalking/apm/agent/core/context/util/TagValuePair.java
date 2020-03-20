/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.skywalking.apm.agent.core.context.util;

import org.apache.skywalking.apm.agent.core.context.tag.AbstractTag;
import org.apache.skywalking.apm.network.common.KeyStringValuePair;

/**
 * 一个键值对
 */
public class TagValuePair {
    /**
     * tag 本身只存储了key的信息 并维护一个添加value 的方法  通过该对象存储tag 以及value的信息
     */
    private AbstractTag key;
    /**
     * tag对应的值
     */
    private String value;

    public TagValuePair(AbstractTag tag, String value) {
        this.key = tag;
        this.value = value;
    }

    public AbstractTag getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    /**
     * 转换成 protobuf 的格式
     * @return
     */
    public KeyStringValuePair transform() {
        KeyStringValuePair.Builder keyValueBuilder = KeyStringValuePair.newBuilder();
        keyValueBuilder.setKey(key.key());
        if (value != null) {
            keyValueBuilder.setValue(value);
        }
        return keyValueBuilder.build();
    }

    public boolean sameWith(AbstractTag tag) {
        return key.isCanOverwrite() && key.getId() == tag.getId();
    }

    public void setValue(String value) {
        this.value = value;
    }
}