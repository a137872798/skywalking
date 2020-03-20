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

import org.apache.skywalking.oap.server.core.storage.ComparableStorageData;

/**
 *
 * @param <STORAGE_DATA>
 */
public class LimitedSizeDataCache<STORAGE_DATA extends ComparableStorageData> extends Window<STORAGE_DATA> implements DataCache {

    /**
     * 存储数据使用的容器
     */
    private SWCollection<STORAGE_DATA> limitedSizeDataCollection;
    /**
     * SWCollection 中 每个key 对应了一组 value 这里是限定 value列表的长度
     */
    private final int limitSize;

    public LimitedSizeDataCache(int limitSize) {
        super(false);
        this.limitSize = limitSize;
        init();
    }

    @Override
    public SWCollection<STORAGE_DATA> collectionInstance() {
        return new LimitedSizeDataCollection<>(limitSize);
    }

    public void add(STORAGE_DATA data) {
        limitedSizeDataCollection.put(data);
    }

    /**
     * 获取当前容器 并且标记成正在写入状态
     */
    @Override
    public void writing() {
        limitedSizeDataCollection = getCurrentAndWriting();
    }

    @Override
    public void finishWriting() {
        limitedSizeDataCollection.finishWriting();
        limitedSizeDataCollection = null;
    }
}

