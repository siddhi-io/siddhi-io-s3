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

package io.siddhi.extension.io.s3.sink;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiAppContext;
import io.siddhi.core.event.Event;
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.common.utils.S3Constants;
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;
import io.siddhi.extension.io.s3.sink.internal.publisher.EventPublisher;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * {@code S3Sink} Handles publishing events to Amazon AWS S3 bucket.
 */
@Extension(
        name = "s3",
        namespace = "sink",
        description = "S3 sink publishes events as Amazon AWS S3 buckets.",
        parameters = {
                @Parameter(
                        name = "credential.provider.class",
                        type = DataType.STRING,
                        description = "AWS credential provider class to be used. If blank along with the username " +
                                "and the password, default credential provider will be used.",
                        optional = true,
                        defaultValue = "EMPTY_STRING"
                ),
                @Parameter(
                        name = "aws.access.key",
                        type = DataType.STRING,
                        description = "AWS access key. This cannot be used along with the credential.provider.class",
                        optional = true,
                        defaultValue = "EMPTY_STRING"
                ),
                @Parameter(
                        name = "aws.secret.key",
                        type = DataType.STRING,
                        description = "AWS secret key. This cannot be used along with the credential.provider.class",
                        optional = true,
                        defaultValue = "EMPTY_STRING"
                ),
                @Parameter(
                        name = "bucket.name",
                        type = DataType.STRING,
                        description = "Name of the S3 bucket"
                ),
                @Parameter(
                        name = "aws.region",
                        type = DataType.STRING,
                        description = "The region to be used to create the bucket",
                        optional = true,
                        defaultValue = "EMPTY_STRING"
                ),
                @Parameter(
                        name = "versioning.enabled",
                        type = DataType.BOOL,
                        description = "Flag to enable versioning support in the bucket",
                        optional = true,
                        defaultValue = "false"
                ),
                @Parameter(
                        name = "object.path",
                        type = DataType.STRING,
                        description = "Path for each S3 object",
                        dynamic = true
                ),
                @Parameter(
                        name = "storage.class",
                        type = DataType.STRING,
                        description = "AWS storage class",
                        optional = true,
                        defaultValue = "standard"
                ),
                @Parameter(
                        name = "content.type",
                        type = DataType.STRING,
                        description = "Content type of the event",
                        optional = true,
                        defaultValue = "application/octet-stream",
                        dynamic = true
                ),
                @Parameter(
                        name = "bucket.acl",
                        type = DataType.STRING,
                        description = "Access control list for the bucket",
                        optional = true,
                        defaultValue = "EMPTY_STRING"
                ),
                @Parameter(
                        name = "node.id",
                        type = DataType.STRING,
                        description = "The node ID of the current publisher. This needs to be unique for each " +
                                "publisher instance as it may cause object overwrites while uploading the objects " +
                                "to same S3 bucket from different publishers.",
                        optional = true,
                        defaultValue = "EMPTY_STRING"
                )
        },
        examples = {
                @Example(
                        syntax = "@sink(type='s3', bucket.name='user-stream-bucket',object.path='bar/users', " +
                                "credential.provider='software.amazon.awssdk.auth.credentials" +
                                ".ProfileCredentialsProvider', flush.size='3',\n" +
                                "    @map(type='json', enclosing.element='$.user', \n" +
                                "        @payload(\"\"\"{\"name\": \"{{name}}\", \"age\": {{age}}}\"\"\"))) \n" +
                                "define stream UserStream(name string, age int);  ",
                        description = "This creates a S3 bucket named 'user-stream-bucket'. Then this will collect " +
                                "3 events together and create a JSON object and save that in S3."
                )
        }
)
public class S3Sink extends Sink<S3Sink.SinkState> {
    private static final Logger logger = Logger.getLogger(S3Sink.class);

    private EventPublisher publisher;
    private SinkConfig config;

    /**
     * Returns the list of classes which the S3 sink can consume. Currently it can consume Strings, Events and
     * ByteBuffers.
     *
     * @return array of supported classes
     */
    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class, Event.class, ByteBuffer.class};
    }

    /**
     * Returns a list of supported dynamic options. Currently the parameter 'objetc.path' can be dynamic.
     *
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{S3Constants.OBJECT_PATH};
    }

    /**
     * The initialization method for {@link S3Sink}. This initializes the SinkConfig built from annotations and the
     * Event publisher.
     *
     * @param streamDefinition the stream definition bind to the {@link S3Sink}
     * @param optionHolder     Option holder containing static and dynamic configuration related
     *                         to the {@link S3Sink}
     * @param configReader     the ConfigReader object
     * @param siddhiAppContext the SiddhiAppContext
     * @return StateFactory for the Function which contains logic for the updated state based on arrived events.
     */
    @Override
    protected StateFactory<SinkState> init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                                           ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        logger.debug("Initializing the S3 sink connector.");
        SinkState state = new SinkState();
        config = new SinkConfig(optionHolder, streamDefinition);

        this.publisher = new EventPublisher(config, optionHolder, state);

        return () -> state;
    }

    /**
     * Publishes events to S3 bucket from the sink.
     *
     * @param payload        payload of the event
     * @param dynamicOptions Option holder containing static and dynamic configuration related to {@link S3Sink}
     * @param state          current state of the sink
     * @throws ConnectionUnavailableException will be thrown when the client cannot be connected.
     */
    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, SinkState state)
            throws ConnectionUnavailableException {
        publisher.publish(payload, dynamicOptions, state);
    }

    /**
     * Initializes the publisher and start all core threads of the publisher thread pool.
     *
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void connect() throws ConnectionUnavailableException {
        config.setMapType(getMapper().getType());
        publisher.init();
        publisher.start();
        logger.debug("Event publisher started.");
    }

    /**
     * Stops the publisher thread pool.
     */
    @Override
    public void disconnect() {
        if (publisher != null) {
            publisher.shutdown();
            logger.debug("Event publisher shutdown.");
        }
    }

    /**
     * Cleanup the sink.
     */
    @Override
    public void destroy() {
    }

    /**
     * Give information to the deployment about the service exposed by the sink. Currently this returns null.
     *
     * @return ServiceDeploymentInfo  Service related information to the deployment
     */
    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    /**
     * State class for S3 sink.
     */
    public class SinkState extends State {
        private BlockingQueue<Runnable> taskQueue;
        private AtomicInteger eventIncrementer;

        public SinkState() {
            this.taskQueue = new LinkedBlockingQueue<>();
            this.eventIncrementer = new AtomicInteger();
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("taskQueue", taskQueue);
            state.put("eventIncrementer", eventIncrementer);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            taskQueue = (BlockingQueue<Runnable>) state.get("taskQueue");
            eventIncrementer = (AtomicInteger) state.get("eventIncrementer");
        }

        public BlockingQueue<Runnable> getTaskQueue() {
            return taskQueue;
        }

        public AtomicInteger getEventIncrementer() {
            return eventIncrementer;
        }
    }
}
