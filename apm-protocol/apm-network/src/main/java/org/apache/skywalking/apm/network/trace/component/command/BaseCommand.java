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

package org.apache.skywalking.apm.network.trace.component.command;

import org.apache.skywalking.apm.network.common.Command;
import org.apache.skywalking.apm.network.common.KeyStringValuePair;

/**
 * 命令对象 基类 将传入的参数填充到command 中
 */
public abstract class BaseCommand {

    private final String command;
    private final String serialNumber;
    /**
     * 用于构建 protobuf 格式的数据
     */
    private final Command.Builder commandBuilder;

    /**
     * @param command  命令描述
     * @param serialNumber
     */
    BaseCommand(String command, String serialNumber) {
        this.command = command;
        this.serialNumber = serialNumber;
        // 就是一个静态方法 返回一个空的Builder实体
        this.commandBuilder = Command.newBuilder();

        // 就是一组普通的键值对 不过使用该builder生成的数据 符合 protobuf的要求
        KeyStringValuePair.Builder arguments = KeyStringValuePair.newBuilder();
        arguments.setKey("SerialNumber");
        arguments.setValue(serialNumber);

        // 设置参数 和  command 对象
        this.commandBuilder.setCommand(command);
        this.commandBuilder.addArgs(arguments);
    }

    Command.Builder commandBuilder() {
        return commandBuilder;
    }

    public String getCommand() {
        return command;
    }

    public String getSerialNumber() {
        return serialNumber;
    }
}
