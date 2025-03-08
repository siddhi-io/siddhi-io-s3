/*
 *  Copyright (c) 2020, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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

package io.siddhi.extension.execution.s3;

import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.annotation.Parameter;
import io.siddhi.annotation.ParameterOverload;
import io.siddhi.annotation.util.DataType;
import io.siddhi.core.config.SiddhiQueryContext;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.executor.ExpressionExecutor;
import io.siddhi.core.query.processor.ProcessingMode;
import io.siddhi.core.query.processor.stream.function.StreamFunctionProcessor;
import io.siddhi.core.util.config.ConfigReader;
import io.siddhi.core.util.snapshot.state.StateFactory;
import io.siddhi.extension.common.S3ServiceClient;
import io.siddhi.extension.common.beans.BucketConfig;
import io.siddhi.extension.common.beans.ClientConfig;
import io.siddhi.extension.common.utils.S3Constants;
import io.siddhi.extension.io.s3.sink.internal.publisher.EventPublisherThreadPoolExecutor;
import io.siddhi.query.api.definition.AbstractDefinition;
import io.siddhi.query.api.definition.Attribute;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import software.amazon.awssdk.services.s3.model.StorageClass;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * {@code S3CopyFunctionProcessor} handles copying object within S3 buckets.
 */
@Extension(
        name = "copy",
        namespace = "s3",
        description = "Copy a file within Amazon AWS S3 buckets.",
        parameters = {
                @Parameter(
                        name = "from.bucket.name",
                        description = "Name of the S3 bucket which is copying from",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "from.key",
                        description = "Key of the object to be copied",
                        type = DataType.STRING,
                        dynamic = true
                ),

                @Parameter(
                        name = "bucket.name",
                        description = "Name of the destination S3 bucket",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "key",
                        description = "Key of the destination object",
                        type = DataType.STRING,
                        dynamic = true
                ),
                @Parameter(
                        name = "async",
                        description = "Toggle async mode",
                        type = DataType.BOOL,
                        dynamic = true,
                        optional = true,
                        defaultValue = "false"
                ),
                @Parameter(
                        name = "credential.provider.class",
                        description = "AWS credential provider class to be used. If blank along with the username " +
                                "and the password, default credential provider will be used.",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "EMPTY_STRING"
                ),
                @Parameter(
                        name = "aws.region",
                        description = "The region to be used to create the bucket",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "EMPTY_STRING"
                ),
                @Parameter(
                        name = "storage.class",
                        description = "AWS storage class for the destination object",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "standard"
                ),
                @Parameter(
                        name = "aws.access.key",
                        description = "AWS access key. This cannot be used along with the credential.provider.class",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "EMPTY_STRING"
                ),
                @Parameter(
                        name = "aws.secret.key",
                        description = "AWS secret key. This cannot be used along with the credential.provider.class",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "EMPTY_STRING"
                ),
                @Parameter(
                        name = "versioning.enabled",
                        description = "Flag to enable versioning support in the destination bucket",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "false"
                ),
                @Parameter(
                        name = "bucket.acl",
                        description = "Access control list for the destination bucket",
                        type = DataType.STRING,
                        optional = true,
                        defaultValue = "EMPTY_STRING"
                )
        },
        parameterOverloads = {
                @ParameterOverload(
                        parameterNames = {"from.bucket.name", "from.key", "bucket.name", "key"}
                ),
                @ParameterOverload(
                        parameterNames = {"from.bucket.name", "from.key", "bucket.name", "key", "async"}
                ),
                @ParameterOverload(
                        parameterNames = {"from.bucket.name", "from.key", "bucket.name", "key", "async",
                                "credential.provider.class"}
                ),
                @ParameterOverload(
                        parameterNames = {"from.bucket.name", "from.key", "bucket.name", "key", "async",
                                "credential.provider.class", "aws.region"}
                ),
                @ParameterOverload(
                        parameterNames = {"from.bucket.name", "from.key", "bucket.name", "key", "async",
                                "credential.provider.class", "aws.region", "storage.class"}
                ),
                @ParameterOverload(
                        parameterNames = {"from.bucket.name", "from.key", "bucket.name", "key", "async",
                                "credential.provider.class", "aws.region", "storage.class", "aws.access.key",
                                "aws.secret.key"}
                ),
                @ParameterOverload(
                        parameterNames = {"from.bucket.name", "from.key", "bucket.name", "key", "async",
                                "credential.provider.class", "aws.region", "storage.class", "aws.access.key",
                                "aws.secret.key", "versioning.enabled"}
                ),
                @ParameterOverload(
                        parameterNames = {"from.bucket.name", "from.key", "bucket.name", "key", "async",
                                "credential.provider.class", "aws.region", "storage.class", "aws.access.key",
                                "aws.secret.key", "versioning.enabled", "bucket.acl"}
                )
        },
        examples = {
                @Example(
                        syntax = "from FooStream#s3:copy('stock-source-bucket', 'stocks.txt', " +
                                "'stock-backup-bucket', '/backup/stocks.txt')",
                        description = "Copy object from one bucket to another."
                )
        }
)
public class S3CopyFunctionProcessor extends StreamFunctionProcessor {
    private static final Logger logger = LogManager.getLogger(S3CopyFunctionProcessor.class);

    private BlockingQueue<Runnable> taskQueue;
    private EventPublisherThreadPoolExecutor executor;

    @Override
    protected Object[] process(Object[] data) {
        if (data.length < 4 || data.length == 9 || data.length > 12) {
            throw new SiddhiAppCreationException("Invalid number of parameters.");
        }

        String[] parameterList = new String[]{
                S3Constants.FROM_BUCKET_NAME,
                S3Constants.FROM_KEY,
                S3Constants.BUCKET_NAME,
                S3Constants.KEY,
                S3Constants.ASYNC,
                S3Constants.CREDENTIAL_PROVIDER_CLASS,
                S3Constants.AWS_REGION,
                S3Constants.STORAGE_CLASS,
                S3Constants.AWS_ACCESS_KEY,
                S3Constants.AWS_SECRET_KEY,
                S3Constants.VERSIONING_ENABLED,
                S3Constants.BUCKET_ACL
        };
        Map<String, Object> parameterMap = new HashMap<>();
        for (int i = 0; i < data.length; i++) {
            parameterMap.put(parameterList[i], data[i]);
        }

        ClientConfig clientConfig = ClientConfig.fromMap(parameterMap);
        BucketConfig bucketConfig = BucketConfig.fromMap(parameterMap);

        String fromBucketName = (String) parameterMap.get(S3Constants.FROM_BUCKET_NAME);
        String fromKey = (String) parameterMap.get(S3Constants.FROM_KEY);
        String key = (String) parameterMap.get(S3Constants.KEY);
        StorageClass storageClass = parameterMap.containsKey(S3Constants.STORAGE_CLASS) ?
                StorageClass.fromValue((String) parameterMap.get(S3Constants.STORAGE_CLASS)) : StorageClass.STANDARD;
        boolean async = (boolean) parameterMap.getOrDefault(S3Constants.ASYNC, false);

        // Validate parameters
        if (fromBucketName == null || fromBucketName.isEmpty()) {
            throw new SiddhiAppCreationException("Parameter '" + S3Constants.FROM_BUCKET_NAME + "' is required.");
        }

        if (fromKey == null || fromKey.isEmpty()) {
            throw new SiddhiAppCreationException("Parameter '" + S3Constants.FROM_KEY + "' is required.");
        }

        clientConfig.validate();
        bucketConfig.validate();

        // Copy object
        CopyTask task = new CopyTask(clientConfig, bucketConfig, fromBucketName, fromKey, key, storageClass);
        if (async) {
            taskQueue.add(task);
        } else {
            task.run();
        }
        return new Object[0];
    }

    @Override
    protected Object[] process(Object data) {
        return process(new Object[]{data});
    }

    @Override
    protected StateFactory init(AbstractDefinition inputDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                ConfigReader configReader, boolean outputExpectsExpiredEvents,
                                SiddhiQueryContext siddhiQueryContext) {
        taskQueue = new LinkedBlockingQueue<>();
        executor = new EventPublisherThreadPoolExecutor(S3Constants.CORE_POOL_SIZE, S3Constants.MAX_POOL_SIZE,
                S3Constants.KEEP_ALIVE_TIME_MS, TimeUnit.MILLISECONDS, taskQueue);
        return null;
    }

    @Override
    public List<Attribute> getReturnAttributes() {
        return new ArrayList<>();
    }

    @Override
    public void start() {
        if (executor != null) {
            executor.prestartAllCoreThreads();
        }
    }

    @Override
    public void stop() {
        if (executor != null) {
            executor.shutdown();
        }
    }

    @Override
    public ProcessingMode getProcessingMode() {
        return ProcessingMode.BATCH;
    }

    class CopyTask implements Runnable {
        private final ClientConfig clientConfig;
        private BucketConfig bucketConfig;
        private String fromBucketName;
        private String fromKey;
        private String key;
        private StorageClass storageClass;

        public CopyTask(ClientConfig clientConfig, BucketConfig bucketConfig, String fromBucketName, String fromKey,
                        String key, StorageClass storageClass) {
            this.clientConfig = clientConfig;
            this.bucketConfig = bucketConfig;
            this.fromBucketName = fromBucketName;
            this.fromKey = fromKey;
            this.key = key;
            this.storageClass = storageClass;
        }

        @Override
        public void run() {
            S3ServiceClient client = new S3ServiceClient(clientConfig);
            client.ensureBucketAvailability(bucketConfig);
            client.copyObject(fromBucketName, fromKey, bucketConfig.getBucketName(), key, storageClass);

            logger.debug("Object '{}' in bucket '{}' is copied to '{}' in bucket '{}'.", fromKey, fromBucketName, key,
                    bucketConfig.getBucketName());
        }
    }
}
