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
import io.siddhi.core.exception.ConnectionUnavailableException;
import io.siddhi.core.stream.ServiceDeploymentInfo;
import io.siddhi.core.stream.output.sink.Sink;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.State;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.core.util.transport.DynamicOptions;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;
import io.siddhi.extension.io.s3.sink.internal.publisher.EventPublisher;
import io.siddhi.extension.io.s3.sink.internal.utils.S3Constants;
import io.siddhi.query.api.definition.StreamDefinition;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * This is a sample class-level comment, explaining what the extension class does.
 * <p>
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Sink configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter", type = "Supprted parameter types.
 *                              eg:{DataType.STRING,DataType.INT, DataType.LONG etc},dynamic=false ,optinal=true/false ,
 *                              if optional =true then assign default value according the type")
 *   System parameter is used to define common extension wide
 *              },
 * examples = {
 * {@literal @}Example({"Example of the first CustomExtension contain syntax and description.Here,
 *                      Syntax describe default mapping for SourceMapper and description describes
 *                      the output of according this syntax},
 *                      }
 * </code></pre>
 * <p>
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Sink configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter", type = "Supprted parameter types.
 *                              eg:{DataType.STRING,DataType.INT, DataType.LONG etc},dynamic=false ,optinal=true/false ,
 *                              if optional =true then assign default value according the type")
 *   System parameter is used to define common extension wide
 *              },
 * examples = {
 * {@literal @}Example({"Example of the first CustomExtension contain syntax and description.Here,
 *                      Syntax describe default mapping for SourceMapper and description describes
 *                      the output of according this syntax},
 *                      }
 * </code></pre>
 * <p>
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Sink configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter", type = "Supprted parameter types.
 *                              eg:{DataType.STRING,DataType.INT, DataType.LONG etc},dynamic=false ,optinal=true/false ,
 *                              if optional =true then assign default value according the type")
 *   System parameter is used to define common extension wide
 *              },
 * examples = {
 * {@literal @}Example({"Example of the first CustomExtension contain syntax and description.Here,
 *                      Syntax describe default mapping for SourceMapper and description describes
 *                      the output of according this syntax},
 *                      }
 * </code></pre>
 * <p>
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Sink configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter", type = "Supprted parameter types.
 *                              eg:{DataType.STRING,DataType.INT, DataType.LONG etc},dynamic=false ,optinal=true/false ,
 *                              if optional =true then assign default value according the type")
 *   System parameter is used to define common extension wide
 *              },
 * examples = {
 * {@literal @}Example({"Example of the first CustomExtension contain syntax and description.Here,
 *                      Syntax describe default mapping for SourceMapper and description describes
 *                      the output of according this syntax},
 *                      }
 * </code></pre>
 */

/**
 * Annotation of Siddhi Extension.
 * <pre><code>
 * eg:-
 * {@literal @}Extension(
 * name = "The name of the extension",
 * namespace = "The namespace of the extension",
 * description = "The description of the extension (optional).",
 * //Sink configurations
 * parameters = {
 * {@literal @}Parameter(name = "The name of the first parameter", type = "Supprted parameter types.
 *                              eg:{DataType.STRING,DataType.INT, DataType.LONG etc},dynamic=false ,optinal=true/false ,
 *                              if optional =true then assign default value according the type")
 *   System parameter is used to define common extension wide
 *              },
 * examples = {
 * {@literal @}Example({"Example of the first CustomExtension contain syntax and description.Here,
 *                      Syntax describe default mapping for SourceMapper and description describes
 *                      the output of according this syntax},
 *                      }
 * </code></pre>
 */

/**
 bucket.acl
 object.acl(check httpheaders)
 object.metadata
 */

@Extension(
        name = "s3",
        namespace = "sink",
        description = " ",
        parameters = {
                @Parameter(
                        name = "credential.provider.class",
                        type = DataType.STRING,
                        description = "AWS credential provider class to be used. If blank along with the username " +
                                "and the password, default credential provider will be used.",
                        optional = true,
                        defaultValue = " "
                ),
                @Parameter(
                        name = "aws.access.key",
                        type = DataType.STRING,
                        description = "AWS access key. This cannot be used along with the credential.provider.class",
                        optional = true,
                        defaultValue = " "
                ),
                @Parameter(
                        name = "aws.secret.key",
                        type = DataType.STRING,
                        description = "AWS secret key. This cannot be used along with the credential.provider.class",
                        optional = true,
                        defaultValue = " "
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
                        defaultValue = " "
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
                        name = "flush.size",
                        type = DataType.INT,
                        description = "Maximum number of events to be written into a file",
                        optional = true,
                        defaultValue = "1"
                ),
//                @Parameter(
//                        name = "rotate.interval.ms",
//                        type = DataType.INT,
//                        description = "Maximum span of event time",
//                        optional = true,
//                        defaultValue = "-1"
//                ),
//                @Parameter(
//                        name = "rotate.scheduled.interval.ms",
//                        type = DataType.INT,
//                        description = "Maximum span of event time from the first event",
//                        optional = true,
//                        defaultValue = "-1"
//                ),
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
                        defaultValue = " "
                )
        },
        examples = {
                @Example(
                        syntax = " ",
                        description = " "
                )
        }
)
// for more information refer https://siddhi-io.github.io/siddhi/documentation/siddhi-5.x/query-guide-5.x/#sink
public class S3Sink extends Sink<S3Sink.SinkState> {
    private static final Logger logger = Logger.getLogger(S3Sink.class);

    private EventPublisher publisher;

    /**
     * Returns the list of classes which this sink can consume.
     * Based on the type of the sink, it may be limited to being able to publish specific type of classes.
     * For example, a sink of type file can only write objects of type String .
     *
     * @return array of supported classes , if extension can support of any types of classes
     * then return empty array .
     */
    @Override
    public Class[] getSupportedInputEventClasses() {
        return new Class[]{String.class};
    }

    /**
     * Returns a list of supported dynamic options (that means for each event value of the option can change) by
     * the transport
     *
     * @return the list of supported dynamic option keys
     */
    @Override
    public String[] getSupportedDynamicOptions() {
        return new String[]{S3Constants.OBJECT_PATH};
    }

    /**
     * The initialization method for {@link Sink}, will be called before other methods. It used to validate
     * all configurations and to get initial values.
     *
     * @param streamDefinition containing stream definition bind to the {@link Sink}
     * @param optionHolder     Option holder containing static and dynamic configuration related
     *                         to the {@link Sink}
     * @param configReader     to read the sink related system configuration.
     * @param siddhiAppContext the context of the {@link io.siddhi.query.api.SiddhiApp} used to
     *                         get siddhi related utility functions.
     * @return StateFactory for the Function which contains logic for the updated state based on arrived events.
     */
    @Override
    protected StateFactory<SinkState> init(StreamDefinition streamDefinition, OptionHolder optionHolder,
                                           ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        logger.debug("Initializing the S3 sink connector.");
        SinkState state = new SinkState();
        SinkConfig config = new SinkConfig(optionHolder, streamDefinition);

        this.publisher = new EventPublisher(config, optionHolder, state);

        return () -> state;
    }

    /**
     * This method will be called when events need to be published via this sink
     *
     * @param payload        payload of the event based on the supported event class exported by the extensions
     * @param dynamicOptions holds the dynamic options of this sink and Use this object to obtain dynamic options.
     * @param state          current state of the sink
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void publish(Object payload, DynamicOptions dynamicOptions, SinkState state)
            throws ConnectionUnavailableException {
        publisher.publish(payload, dynamicOptions, state);
    }

    /**
     * This method will be called before the processing method.
     * Intention to establish connection to publish event.
     *
     * @throws ConnectionUnavailableException if end point is unavailable the ConnectionUnavailableException thrown
     *                                        such that the  system will take care retrying for connection
     */
    @Override
    public void connect() throws ConnectionUnavailableException {
        publisher.start();
        logger.debug("Event publisher started.");
    }

    /**
     * Called after all publishing is done, or when {@link ConnectionUnavailableException} is thrown
     * Implementation of this method should contain the steps needed to disconnect from the sink.
     */
    @Override
    public void disconnect() {
        if (publisher != null) {
            publisher.shutdown();
            logger.debug("Event publisher shutdown.");
        }
    }

    /**
     * The method can be called when removing an event receiver.
     * The cleanups that have to be done after removing the receiver could be done here.
     */
    @Override
    public void destroy() {
        // Not applicable
    }

    /**
     * Give information to the deployment about the service exposed by the sink.
     *
     * @return ServiceDeploymentInfo  Service related information to the deployment
     */
    @Override
    protected ServiceDeploymentInfo exposeServiceDeploymentInfo() {
        return null;
    }

    public class SinkState extends State {
        private BlockingQueue<Runnable> taskQueue;
        private Map<String, Object> stateMaps;

        public SinkState() {
            this.taskQueue = new LinkedBlockingQueue<>();
            this.stateMaps = new HashMap<>();
        }

        @Override
        public boolean canDestroy() {
            return false;
        }

        @Override
        public Map<String, Object> snapshot() {
            Map<String, Object> state = new HashMap<>();
            state.put("taskQueue", taskQueue);
            state.put("stateMaps", stateMaps);
            return state;
        }

        @Override
        public void restore(Map<String, Object> state) {
            taskQueue = (BlockingQueue<Runnable>) state.get("taskQueue");
            stateMaps = (Map<String, Object>) state.get("stateMaps");
        }

        public BlockingQueue<Runnable> getTaskQueue() {
            return taskQueue;
        }

        public Object getStateMap(String key) {
            return stateMaps.get(key);
        }

        public void setStateMaps(String key, Object map) {
            stateMaps.put(key, map);
        }
    }
}
