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

import io.siddhi.extension.common.S3ServiceClient;
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;
import io.siddhi.extension.io.s3.sink.internal.utils.MapperTypes;

import java.nio.file.Paths;

/**
 * {@code PublisherTask} is responsible for each event object to be uploaded to a S3 bucket.
 */
public class PublisherTask implements Runnable {

    private final S3ServiceClient client;
    private final int offset;

    private SinkConfig sinkConfig;
    private Object payload;
    private String objectPath;

    public PublisherTask(S3ServiceClient client, SinkConfig sinkConfig, String objectPath, Object payload, int offset) {
        this.client = client;
        this.sinkConfig = sinkConfig;
        this.objectPath = objectPath;
        this.payload = payload;
        this.offset = offset;
    }

    @Override
    public void run() {
        client.uploadObject(sinkConfig.getBucketConfig().getBucketName(), buildkey(), payload,
                sinkConfig.getContentType(), sinkConfig.getStorageClass());
    }

    private String buildkey() {
        String extension = MapperTypes.forName(sinkConfig.getMapType()).getExtension();
        String key = (sinkConfig.getNodeId() != null && !sinkConfig.getNodeId().isEmpty()) ?
                String.format("%s-%s-%d.%s", sinkConfig.getStreamId(), sinkConfig.getNodeId(), offset, extension) :
                String.format("%s-%d.%s", sinkConfig.getStreamId(), offset, extension);
        return Paths.get(objectPath, key).toString();
    }
}
