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

package org.apache.skywalking.oap.server.core.remote.client;

import lombok.Getter;
import lombok.Setter;
import org.apache.skywalking.oap.server.core.Const;

/**
 * oap 对应client端 每个被skywalking 监控的应用会定期将链路信息发送到 oap上
 */
@Getter
public class Address implements Comparable<Address> {
    private final String host;
    private final int port;
    /**
     * oap 是否跟应用在同一台机器上
     */
    @Setter
    private volatile boolean isSelf;

    public Address(String host, int port, boolean isSelf) {
        this.host = host;
        this.port = port;
        this.isSelf = isSelf;
    }

    @Override
    public int hashCode() {
        return toString().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;

        Address address = (Address) obj;
        return host.equals(address.host) && port == address.port;
    }

    @Override
    public String toString() {
        return host + Const.ID_SPLIT + port;
    }

    @Override
    public int compareTo(Address o) {
        return this.toString().compareTo(o.toString());
    }
}
