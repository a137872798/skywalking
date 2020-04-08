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

package org.apache.skywalking.oap.server.core.analysis.worker;

import java.io.IOException;
import org.apache.skywalking.oap.server.core.analysis.record.Record;
import org.apache.skywalking.oap.server.core.storage.IBatchDAO;
import org.apache.skywalking.oap.server.core.storage.IRecordDAO;
import org.apache.skywalking.oap.server.core.storage.StorageModule;
import org.apache.skywalking.oap.server.core.storage.model.Model;
import org.apache.skywalking.oap.server.core.worker.AbstractWorker;
import org.apache.skywalking.oap.server.library.client.request.InsertRequest;
import org.apache.skywalking.oap.server.library.module.ModuleDefineHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RecordPersistentWorker extends AbstractWorker<Record> {

    private static final Logger logger = LoggerFactory.getLogger(RecordPersistentWorker.class);

    private final Model model;
    private final IRecordDAO recordDAO;
    private final IBatchDAO batchDAO;

    RecordPersistentWorker(ModuleDefineHolder moduleDefineHolder, Model model, IRecordDAO recordDAO) {
        super(moduleDefineHolder);
        this.model = model;
        this.recordDAO = recordDAO;
        this.batchDAO = moduleDefineHolder.find(StorageModule.NAME).provider().getService(IBatchDAO.class);
    }

    /**
     * RecordStreamProcessor 会间接触发该方法
     * @param record
     */
    @Override
    public void in(Record record) {
        try {
            // 这里返回的对象 就是 sql 和 connection  param的包装对象  本次会话还没有执行 但是必备条件已充足
            InsertRequest insertRequest = recordDAO.prepareBatchInsert(model, record);
            // 实际上 batchDao 内部也是一个 生产者-消费者模型  也就是说skywalking 本身是一个  多生产者单消费模型 尽可能的通过批处理来提高效率
            batchDAO.asynchronous(insertRequest);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
    }
}

