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
import io.siddhi.extension.common.S3ServiceClient;
import io.siddhi.extension.common.utils.S3Constants;
import io.siddhi.extension.io.s3.sink.S3Sink;
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * Handles queueing events for publish to S3.
 */
public class EventPublisher {
    private static final Logger logger = Logger.getLogger(EventPublisher.class);

    private final SinkConfig config;
    private final S3Sink.SinkState state;

    private S3ServiceClient client;
    private OptionHolder optionHolder;
    private EventPublisherThreadPoolExecutor executor;

    public EventPublisher(SinkConfig config, OptionHolder optionHolder, S3Sink.SinkState state) {
        this.optionHolder = optionHolder;
        this.config = config;
        this.state = state;
    }

    public void init() {
        client = new S3ServiceClient(config.getClientConfig());
        executor = new EventPublisherThreadPoolExecutor(S3Constants.CORE_POOL_SIZE, S3Constants.MAX_POOL_SIZE,
                S3Constants.KEEP_ALIVE_TIME_MS, TimeUnit.MILLISECONDS, state.getTaskQueue());
    }

    public void start() {
        logger.debug("Starting all core threads.");
        executor.prestartAllCoreThreads();
    }

    public void publish(Object payload, DynamicOptions dynamicOptions, S3Sink.SinkState state) {
        String objectPath = optionHolder.validateAndGetOption(S3Constants.OBJECT_PATH).getValue(dynamicOptions);
        state.getTaskQueue().add(new PublisherTask(client, config, objectPath, payload,
                state.getEventIncrementer().incrementAndGet()));
        logger.debug("The event is being added to the queue. Will be published to S3 shortly.");
    }

    public void shutdown() {
        logger.debug("Shutting down worker threads.");
        if (executor != null) {
            executor.shutdown();
        }
    }
}
