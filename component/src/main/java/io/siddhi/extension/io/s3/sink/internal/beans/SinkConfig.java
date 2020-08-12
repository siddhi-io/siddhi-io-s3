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

package io.siddhi.extension.io.s3.sink.internal.beans;

import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.s3.sink.internal.utils.S3Constants;
import io.siddhi.extension.common.beans.BucketConfig;
import io.siddhi.extension.common.beans.ClientConfig;
import io.siddhi.query.api.definition.StreamDefinition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.StorageClass;

/**
 * Holds sink configurations read from annotations.
 */
public class SinkConfig {
    private String nodeId = null;
    private String contentType = S3Constants.DEFAULT_CONTENT_TYPE;
    private String streamId;
    private String mapType;
    private StorageClass storageClass = StorageClass.STANDARD;

    private ClientConfig clientConfig;
    private BucketConfig bucketConfig;

    public SinkConfig(OptionHolder optionHolder, StreamDefinition streamDefinition) {
        clientConfig = new ClientConfig();
        bucketConfig = new BucketConfig();
        optionHolder.getStaticOptionsKeys().forEach(key -> {
            switch (key) {
                case S3Constants.CREDENTIAL_PROVIDER_CLASS:
                    clientConfig.setCredentialProviderClass(
                            optionHolder.validateAndGetStaticValue(S3Constants.CREDENTIAL_PROVIDER_CLASS));
                    break;
                case S3Constants.AWS_ACCESS_KEY:
                    clientConfig.setAwsAccessKey(optionHolder.validateAndGetStaticValue(S3Constants.AWS_ACCESS_KEY));
                    break;
                case S3Constants.AWS_SECRET_KEY:
                    clientConfig.setAwsSecretKey(optionHolder.validateAndGetStaticValue(S3Constants.AWS_SECRET_KEY));
                    break;
                case S3Constants.BUCKET_NAME:
                    bucketConfig.setBucketName(optionHolder.validateAndGetStaticValue(S3Constants.BUCKET_NAME));
                    break;
                case S3Constants.BUCKET_ACL:
                    bucketConfig.setBucketAcl(optionHolder.validateAndGetStaticValue(S3Constants.BUCKET_ACL));
                    break;
                case S3Constants.NODE_ID:
                    nodeId = optionHolder.validateAndGetStaticValue(S3Constants.NODE_ID);
                    break;
                case S3Constants.CONTENT_TYPE:
                    contentType = optionHolder.validateAndGetStaticValue(S3Constants.CONTENT_TYPE);
                    break;
                case S3Constants.STORAGE_CLASS:
                    String storageClassName = optionHolder.validateAndGetStaticValue(S3Constants.STORAGE_CLASS);
                    try {
                        storageClass = StorageClass.fromValue(storageClassName);
                    } catch (IllegalArgumentException e) {
                        throw new SiddhiAppCreationException("Invalid value defined for the field 'storage.class.'");
                    }
                    break;
                case S3Constants.AWS_REGION:
                    String regionName = optionHolder.validateAndGetStaticValue(S3Constants.AWS_REGION);
                    clientConfig.setAwsRegion(Region.of(regionName));
                    break;
                case S3Constants.VERSIONING_ENABLED:
                    bucketConfig.setVersioningEnabled(Boolean.parseBoolean(
                            optionHolder.validateAndGetStaticValue(S3Constants.VERSIONING_ENABLED)));
                    break;
                default:
                    // Ignore! Not a valid option.
            }
        });

        if (bucketConfig.getBucketName() == null || bucketConfig.getBucketName().isEmpty()) {
            throw new SiddhiAppCreationException("Parameter 'bucket.name' is required.");
        }

        if (!optionHolder.isOptionExists(S3Constants.OBJECT_PATH)) {
            throw new SiddhiAppCreationException("Parameter 'object.path' is required.");
        }

        // Get stream id and the map type from stream definition.
        streamId = streamDefinition.getId();
    }

    public String getNodeId() {
        return nodeId;
    }

    public String getContentType() {
        return contentType;
    }

    public String getStreamId() {
        return streamId;
    }

    public String getMapType() {
        return mapType;
    }

    public void setMapType(String mapType) {
        this.mapType = mapType;
    }

    public StorageClass getStorageClass() {
        return storageClass;
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public BucketConfig getBucketConfig() {
        return bucketConfig;
    }
}
