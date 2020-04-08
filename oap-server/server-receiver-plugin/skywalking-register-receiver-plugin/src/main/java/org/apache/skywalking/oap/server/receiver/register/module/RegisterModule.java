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

package org.apache.skywalking.oap.server.receiver.register.module;

import org.apache.skywalking.oap.server.library.module.ModuleDefine;

/**
 * 该模块用于管理服务实例注册   比如某个应用使用了探针后 会通过ServiceAndEndpointRegisterClient 将服务名称传过来 然后由该对象分配一个id
 * ip应该是要在集群范围内唯一吧 没有跟其他节点通信的前提下如何创建唯一id ???
 */
public class RegisterModule extends ModuleDefine {

    public RegisterModule() {
        super("receiver-register");
    }

    @Override
    public Class[] services() {
        return new Class[0];
    }
}
