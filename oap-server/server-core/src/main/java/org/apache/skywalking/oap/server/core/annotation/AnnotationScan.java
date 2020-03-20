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

package org.apache.skywalking.oap.server.core.annotation;

import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * Scan the annotation, and notify the listener(s)
 * 该对象用于扫描注解 并通知相应的监听器
 */
public class AnnotationScan {

    private final List<AnnotationListenerCache> listeners;

    public AnnotationScan() {
        this.listeners = new LinkedList<>();
    }

    /**
     * Register the callback listener
     *
     * @param listener to be called after class found w/ annotation
     *                 监听器对象首先被包装成一个 缓存 保存在list中 当扫描之后才将获取到的类通知到 监听器上
     */
    public void registerListener(AnnotationListener listener) {
        listeners.add(new AnnotationListenerCache(listener));
    }

    /**
     * Begin to scan classes.
     */
    public void scan() throws IOException {
        // 获取当前类加载器对应的路径
        ClassPath classpath = ClassPath.from(this.getClass().getClassLoader());
        // 找到 skywalking包 并获取下面所有的类信息
        ImmutableSet<ClassPath.ClassInfo> classes = classpath.getTopLevelClassesRecursive("org.apache.skywalking");
        for (ClassPath.ClassInfo classInfo : classes) {
            Class<?> aClass = classInfo.load();

            for (AnnotationListenerCache listener : listeners) {
                // 如果扫描到的类 包含了该监听器要求的注解
                if (aClass.isAnnotationPresent(listener.annotation())) {
                    listener.addMatch(aClass);
                }
            }
        }

        listeners.forEach(AnnotationListenerCache::complete);
    }

    private class AnnotationListenerCache {
        private AnnotationListener listener;

        /**
         * 当前维护的一组 满足触发监听器条件的类 比如某个listener 监听的是@A  然后该列表内部会维护一组携带@A注解的类
         */
        private List<Class<?>> matchedClass;

        private AnnotationListenerCache(AnnotationListener listener) {
            this.listener = listener;
            matchedClass = new LinkedList<>();
        }

        private Class<? extends Annotation> annotation() {
            return this.listener.annotation();
        }

        private void addMatch(Class aClass) {
            matchedClass.add(aClass);
        }

        private void complete() {
            matchedClass.sort(Comparator.comparing(Class::getName));
            matchedClass.forEach(aClass -> listener.notify(aClass));
        }
    }
}
