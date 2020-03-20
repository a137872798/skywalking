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

package org.apache.skywalking.oap.server.core.remote.selector;

import java.util.List;
import org.apache.skywalking.oap.server.core.remote.client.RemoteClient;
import org.apache.skywalking.oap.server.core.remote.data.StreamData;

/**
 * 随机选择一个节点
 */
public class RollingSelector implements RemoteClientSelector {

    /**
     * 这里也没有强制要求可见性
     */
    private int index = 0;

    @Override
    public RemoteClient select(List<RemoteClient> clients, StreamData streamData) {
        int size = clients.size();
        index++;
        int selectIndex = Math.abs(index) % size;

        if (index == Integer.MAX_VALUE) {
            index = 0;
        }
        return clients.get(selectIndex);
    }
}
