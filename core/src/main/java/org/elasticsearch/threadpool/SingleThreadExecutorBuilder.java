/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.threadpool;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.node.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;

/**
 * A builder for fixed executors.
 */
public final class SingleThreadExecutorBuilder extends ExecutorBuilder<SingleThreadExecutorBuilder.ExecutorSettings> {

    private final int queueSize;

    /**
     * Construct a fixed executor builder.
     *
     * @param settings  the node-level settings
     * @param name      the name of the executor
     * @param queueSize the size of the backing queue, -1 for unbounded
     */
    SingleThreadExecutorBuilder(final Settings settings, final String name, int queueSize) {
        super(name);
        this.queueSize = queueSize;
    }

    @Override
    public List<Setting<?>> getRegisteredSettings() {
        return new ArrayList<>();
    }

    @Override
    ExecutorSettings getSettings(Settings settings) {
        final String nodeName = Node.NODE_NAME_SETTING.get(settings);
        return new ExecutorSettings(nodeName, queueSize);
    }

    @Override
    ThreadPool.ExecutorHolder build(final ExecutorSettings settings, final ThreadContext threadContext) {
        int queueSize = settings.queueSize;
        final ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(EsExecutors.threadName(settings.nodeName, name()));
        final ExecutorService executor = EsExecutors.newFixed(name(), 1, queueSize, threadFactory, threadContext);
        final ThreadPool.Info info =
            new ThreadPool.Info(name(), ThreadPool.ThreadPoolType.FIXED, 1, 1, null, queueSize < 0 ? null : new SizeValue(queueSize));
        return new ThreadPool.ExecutorHolder(executor, info);
    }

    @Override
    String formatInfo(ThreadPool.Info info) {
        return String.format(
            Locale.ROOT,
            "name [%s], size [%d], queue size [%s]",
            info.getName(),
            info.getMax(),
            info.getQueueSize() == null ? "unbounded" : info.getQueueSize());
    }

    static class ExecutorSettings extends ExecutorBuilder.ExecutorSettings {

        private final int queueSize;

        ExecutorSettings(final String nodeName, final int queueSize) {
            super(nodeName);
            this.queueSize = queueSize;
        }

    }

}
