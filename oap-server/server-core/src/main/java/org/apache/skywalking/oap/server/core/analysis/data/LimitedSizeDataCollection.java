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

package org.apache.skywalking.oap.server.core.analysis.data;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import org.apache.skywalking.oap.server.core.storage.ComparableStorageData;

/**
 * 数据容器 而缓存就是该对象的一层外包装
 * @param <STORAGE_DATA>
 */
public class LimitedSizeDataCollection<STORAGE_DATA extends ComparableStorageData> implements SWCollection<STORAGE_DATA> {

    private final HashMap<STORAGE_DATA, LinkedList<STORAGE_DATA>> data;
    private final int limitedSize;
    private volatile boolean writing;
    private volatile boolean reading;

    LimitedSizeDataCollection(int limitedSize) {
        this.data = new HashMap<>();
        this.writing = false;
        this.reading = false;
        this.limitedSize = limitedSize;
    }

    @Override
    public void finishWriting() {
        writing = false;
    }

    @Override
    public void writing() {
        writing = true;
    }

    @Override
    public boolean isWriting() {
        return writing;
    }

    @Override
    public void finishReading() {
        reading = false;
    }

    @Override
    public void reading() {
        reading = true;
    }

    @Override
    public boolean isReading() {
        return reading;
    }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public void clear() {
        data.clear();
    }

    @Override
    public boolean containsKey(STORAGE_DATA key) {
        throw new UnsupportedOperationException("Limited size data collection doesn't support containsKey operation.");
    }

    @Override
    public STORAGE_DATA get(STORAGE_DATA key) {
        throw new UnsupportedOperationException("Limited size data collection doesn't support get operation.");
    }

    /**
     * 将某种类型的数据保存到容器中
     * @param value
     */
    @Override
    public void put(STORAGE_DATA value) {
        // 该数据结构是用于实现 TopN的
        LinkedList<STORAGE_DATA> storageDataList = this.data.get(value);
        if (storageDataList == null) {
            storageDataList = new LinkedList<>();
            data.put(value, storageDataList);
        }

        // 在没有达到限制值前 可以正常添加
        if (storageDataList.size() < limitedSize) {
            storageDataList.add(value);
            return;
        }

        // 从头开始遍历每个值 只保留最大的值
        for (int i = 0; i < storageDataList.size(); i++) {
            STORAGE_DATA storageData = storageDataList.get(i);
            if (value.compareTo(storageData) <= 0) {
                if (i == 0) {
                    // input value is less than the smallest in top N list, ignore
                    // 当前值已经是最小的了 就不允许加入到容器中
                } else {
                    // Remove the smallest in top N list
                    // add the current value into the right position
                    // 这个值会挤掉 某个更小的值
                    storageDataList.add(i, value);
                    storageDataList.removeFirst();
                }
                return;
            }
        }

        // Add the value as biggest in top N list  代表写入的值最大  那么就移除第一个值
        storageDataList.addLast(value);
        storageDataList.removeFirst();
    }

    @Override
    public Collection<STORAGE_DATA> collection() {
        List<STORAGE_DATA> collection = new ArrayList<>();
        data.values().forEach(e -> e.forEach(collection::add));
        return collection;
    }
}
