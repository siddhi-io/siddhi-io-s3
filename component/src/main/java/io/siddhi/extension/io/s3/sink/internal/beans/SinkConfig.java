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
import io.siddhi.query.api.definition.StreamDefinition;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.StorageClass;

/**
 * Holds sink configurations read from annotations.
 */
public class SinkConfig {
    private String credentialProviderClass = null;
    private String awsAccessKey = null;
    private String awsSecretKey = null;
    private String bucketName = null;
    private String bucketAcl = null;
    private String nodeId = null;
    private String contentType = S3Constants.DEFAULT_CONTENT_TYPE;
    private String streamId;
    private String mapType;
    private StorageClass storageClass = StorageClass.STANDARD;
    private Region awsRegion = Region.US_WEST_2;
    private boolean versioningEnabled = false;

    public SinkConfig(OptionHolder optionHolder, StreamDefinition streamDefinition) {
        optionHolder.getStaticOptionsKeys().forEach(key -> {
            switch (key) {
                case S3Constants.CREDENTIAL_PROVIDER_CLASS:
                    credentialProviderClass =
                            optionHolder.validateAndGetStaticValue(S3Constants.CREDENTIAL_PROVIDER_CLASS);
                    break;
                case S3Constants.AWS_ACCESS_KEY:
                    awsAccessKey = optionHolder.validateAndGetStaticValue(S3Constants.AWS_ACCESS_KEY);
                    break;
                case S3Constants.AWS_SECRET_KEY:
                    awsSecretKey = optionHolder.validateAndGetStaticValue(S3Constants.AWS_SECRET_KEY);
                    break;
                case S3Constants.BUCKET_NAME:
                    bucketName = optionHolder.validateAndGetStaticValue(S3Constants.BUCKET_NAME);
                    break;
                case S3Constants.BUCKET_ACL:
                    bucketAcl = optionHolder.validateAndGetStaticValue(S3Constants.BUCKET_ACL);
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
                        throw new SiddhiAppCreationException("Invalid valid defined for the field 'storage.class.'");
                    }
                    break;
                case S3Constants.AWS_REGION:
                    String regionName = optionHolder.validateAndGetStaticValue(S3Constants.AWS_REGION);
                    awsRegion = Region.of(regionName);
                    break;
                case S3Constants.VERSIONING_ENABLED:
                    versioningEnabled = Boolean.parseBoolean(
                            optionHolder.validateAndGetStaticValue(S3Constants.VERSIONING_ENABLED));
                    break;
                default:
                    // Ignore! Not a valid option.
            }
        });

        if (bucketName == null || bucketName.isEmpty()) {
            throw new SiddhiAppCreationException("Parameter 'bucket.name' is required.");
        }

        if (!optionHolder.isOptionExists(S3Constants.OBJECT_PATH)) {
            throw new SiddhiAppCreationException("Parameter 'object.path' is required.");
        }

        // Get stream id and the map type from stream definition.
        streamId = streamDefinition.getId();
    }

    public String getCredentialProviderClass() {
        return credentialProviderClass;
    }

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public String getBucketName() {
        return bucketName;
    }

    public String getBucketAcl() {
        return bucketAcl;
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

    public Region getAwsRegion() {
        return awsRegion;
    }

    public boolean isVersioningEnabled() {
        return versioningEnabled;
    }
}
