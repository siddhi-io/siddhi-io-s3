/*
 *  Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied. See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package io.siddhi.extension.io.s3.sink.internal.publisher;

import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.s3.sink.S3Sink;
import io.siddhi.extension.io.s3.sink.internal.RotationStrategy;
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;
import io.siddhi.extension.io.s3.sink.internal.strategies.countbased.CountBasedRotationStrategy;
import io.siddhi.extension.io.s3.sink.internal.utils.S3Constants;
import io.siddhi.extension.io.s3.sink.internal.ServiceClient;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class EventPublisher {

    private static final Logger logger = Logger.getLogger(EventPublisher.class);
    private static final int CORE_POOL_SIZE = 10;
    private static final int MAX_POOL_SIZE = 20;
    private static final int KEEP_ALIVE_TIME_MS = 5000;

    private ServiceClient client;
    private RotationStrategy rotationStrategy;
    private OptionHolder optionHolder;
    private EventPublisherThreadPoolExecutor executor;

    public EventPublisher(SinkConfig config, OptionHolder optionHolder, S3Sink.SinkState state) {
        this.optionHolder = optionHolder;
        this.client = new ServiceClient(config);
        this.rotationStrategy = getRotationStrategy();
        this.rotationStrategy.init(config, this.client, state);

        this.executor = new EventPublisherThreadPoolExecutor(CORE_POOL_SIZE, MAX_POOL_SIZE, KEEP_ALIVE_TIME_MS,
                TimeUnit.MILLISECONDS, state.getTaskQueue());
    }

    public void start() {
        logger.debug("Starting all core threads.");
        this.executor.prestartAllCoreThreads();
    }

    public void publish(Object payload, DynamicOptions dynamicOptions, S3Sink.SinkState state) {
        String objectPath = optionHolder.validateAndGetOption(S3Constants.OBJECT_PATH).getValue(dynamicOptions);
        logger.debug("Queuing the event for publishing: " + payload);
        rotationStrategy.queueEvent(objectPath, payload);
    }

    public void shutdown() {
        logger.debug("Shutting down worker threads.");
        if (executor != null) {
            executor.shutdown();
        }
    }

    private RotationStrategy getRotationStrategy() {
        RotationStrategy strategy = new CountBasedRotationStrategy();
        logger.debug(strategy.getName() + " rotation strategy is selected.");
        return strategy;
    }
}
