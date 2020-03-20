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

package org.apache.skywalking.apm.commons.datacarrier.common;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class AtomicRangeInteger extends Number implements Serializable {
    private static final long serialVersionUID = -4099792402691141643L;
    /**
     * 一个原子数组
     */
    private AtomicIntegerArray values;

    private static final int VALUE_OFFSET = 15;

    private int startValue;
    private int endValue;

    /**
     * 该数组最大长度为 maxValue
     * @param startValue
     * @param maxValue
     */
    public AtomicRangeInteger(int startValue, int maxValue) {
        this.values = new AtomicIntegerArray(31);
        // 在下标为15的位置设置 初始值
        this.values.set(VALUE_OFFSET, startValue);
        this.startValue = startValue;
        this.endValue = maxValue - 1;
    }

    public final int getAndIncrement() {
        int next;
        do {
            // 获取下标为15的位置的值
            next = this.values.incrementAndGet(VALUE_OFFSET);
            // 当本次添加超过了 最大值时 将值重置为起始值
            if (next > endValue && this.values.compareAndSet(VALUE_OFFSET, next, startValue)) {
                return endValue;
            }
        }
        while (next > endValue);

        return next - 1;
    }

    public final int get() {
        return this.values.get(VALUE_OFFSET);
    }

    public int intValue() {
        return this.values.get(VALUE_OFFSET);
    }

    public long longValue() {
        return this.values.get(VALUE_OFFSET);
    }

    public float floatValue() {
        return this.values.get(VALUE_OFFSET);
    }

    public double doubleValue() {
        return this.values.get(VALUE_OFFSET);
    }
}
