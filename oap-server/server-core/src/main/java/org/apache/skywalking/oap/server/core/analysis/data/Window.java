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

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Data cache window. Window holds two data collections(A and B). They are switchable based on outside command. In any
 * time, one collection is accepting the input data, and the other is immutable.
 *
 * This window makes sure there is not concurrency read-write situation.
 *
 * @param <DATA> type in the Window and internal collection.
 */
public abstract class Window<DATA> {

    /**
     * 代表当前使用 A 窗口 还是 B 窗口
     */
    private AtomicInteger windowSwitch = new AtomicInteger(0);

    // 代表当前指向的容器
    private SWCollection<DATA> pointer;

    // 使用2个容器 这样就可以做到读写分离
    private SWCollection<DATA> windowDataA;
    private SWCollection<DATA> windowDataB;

    Window() {
        this(true);
    }

    Window(boolean autoInit) {
        if (autoInit) {
            init();
        }
    }

    /**
     * 初始化内部的数据容器
     */
    protected void init() {
        this.windowDataA = collectionInstance();
        this.windowDataB = collectionInstance();
        // 首先指向A 容器
        this.pointer = windowDataA;
    }

    public abstract SWCollection<DATA> collectionInstance();

    public boolean trySwitchPointer() {
        return windowSwitch.incrementAndGet() == 1 && !getLast().isReading();
    }

    public void trySwitchPointerFinally() {
        windowSwitch.addAndGet(-1);
    }

    /**
     * 切换当前point 指向的容器
     */
    public void switchPointer() {
        if (pointer == windowDataA) {
            pointer = windowDataB;
        } else {
            pointer = windowDataA;
        }
        // 将上一个容器切换成正在读取的状态
        getLast().reading();
    }

    /**
     * 返回当前容器 并且标记成正在写入的状态
     * @return
     */
    SWCollection<DATA> getCurrentAndWriting() {
        if (pointer == windowDataA) {
            windowDataA.writing();
            return windowDataA;
        } else {
            windowDataB.writing();
            return windowDataB;
        }
    }

    private SWCollection<DATA> getCurrent() {
        return pointer;
    }

    public int currentCollectionSize() {
        return getCurrent().size();
    }

    /**
     * 返回另一个容器 (非pointer 指向的)
     * @return
     */
    public SWCollection<DATA> getLast() {
        if (pointer == windowDataA) {
            return windowDataB;
        } else {
            return windowDataA;
        }
    }

    /**
     * 将另一个容器的读取标识取消
     */
    public void finishReadingLast() {
        getLast().clear();
        getLast().finishReading();
    }
}
