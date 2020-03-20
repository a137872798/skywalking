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
 */

package org.apache.skywalking.oap.server.core.storage.model;

import java.lang.reflect.Field;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import lombok.Getter;
import org.apache.skywalking.oap.server.core.source.DefaultScopeDefine;
import org.apache.skywalking.oap.server.core.storage.annotation.Column;
import org.apache.skywalking.oap.server.core.storage.annotation.Storage;
import org.apache.skywalking.oap.server.core.storage.annotation.ValueColumnMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 每个父级接口都对应一种服务 代表该对象可以get/set/override 模块
 * 相当于对module 做一个统一管理
 */
public class StorageModels implements IModelGetter, IModelSetter, IModelOverride {

    private static final Logger logger = LoggerFactory.getLogger(StorageModels.class);

    @Getter
    private final List<Model> models;

    public StorageModels() {
        this.models = new LinkedList<>();
    }

    /**
     * 增加一对映射关系
     * @param aClass  每一种数据类型 对应一个class 而上面的 @Stream 注解对应唯一的scopeId
     * @param scopeId
     * @param storage  存储信息
     * @param record
     * @return
     */
    @Override
    public Model putIfAbsent(Class aClass, int scopeId, Storage storage, boolean record) {
        // Check this scope id is valid.
        // 携带@Stream注解的类 还会额外携带一个 @ScopeDeclaration 用于定义该类的scope 也是通过注解监听器的方式进行注册
        DefaultScopeDefine.nameOf(scopeId);

        // 如果storage对应的模块已经存在 就直接返回
        for (Model model : models) {
            if (model.getName().equals(storage.getModelName())) {
                return model;
            }
        }

        List<ModelColumn> modelColumns = new LinkedList<>();
        // 从模块中抽取对应的字段
        retrieval(aClass, storage.getModelName(), modelColumns);

        // 将模块名 所有模块字段 等等信息生成model
        Model model = new Model(storage.getModelName(), modelColumns, storage.isCapableOfTimeSeries(), storage.isDeleteHistory(), scopeId, storage
            .getDownsampling(), record);
        models.add(model);

        return model;
    }

    /**
     * 从目标类中抽取字段 并添加到容器中
     * @param clazz
     * @param modelName
     * @param modelColumns
     */
    private void retrieval(Class clazz, String modelName, List<ModelColumn> modelColumns) {
        Field[] fields = clazz.getDeclaredFields();

        for (Field field : fields) {
            // 抽取字段上的注解信息 并生成 ModelColumn 对象
            if (field.isAnnotationPresent(Column.class)) {
                Column column = field.getAnnotation(Column.class);
                modelColumns.add(new ModelColumn(new ColumnName(column.columnName()), field.getType(), column.matchQuery(), column
                    .content()));
                if (logger.isDebugEnabled()) {
                    logger.debug("The field named {} with the {} type", column.columnName(), field.getType());
                }
                // 代表支持查询 那么在 管理器中添加关联关系
                if (column.isValue()) {
                    ValueColumnMetadata.INSTANCE.putIfAbsent(modelName, column.columnName(), column.function());
                }
            }
        }

        // 递归获取所有信息
        if (Objects.nonNull(clazz.getSuperclass())) {
            retrieval(clazz.getSuperclass(), modelName, modelColumns);
        }
    }

    @Override
    public void overrideColumnName(String columnName, String newName) {
        models.forEach(model -> model.getColumns().forEach(column -> {
            ColumnName existColumnName = column.getColumnName();
            String name = existColumnName.getName();
            if (name.equals(columnName)) {
                existColumnName.setStorageName(newName);
                logger.debug("Model {} column {} has been override. The new column name is {}.", model.getName(), name, newName);
            }
        }));
    }
}
