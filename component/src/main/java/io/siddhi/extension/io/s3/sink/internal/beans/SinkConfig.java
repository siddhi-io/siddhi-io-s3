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

import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.StorageClass;
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.util.transport.OptionHolder;
import io.siddhi.extension.io.s3.sink.internal.utils.S3Constants;
import io.siddhi.query.api.definition.StreamDefinition;

public class SinkConfig {
    private String storageClass = StorageClass.Standard.toString();
    private String credentialProviderClass = null;
    private String awsAccessKey = null;
    private String awsSecretKey = null;
    private String bucketName = null;
    private String awsRegion = Regions.DEFAULT_REGION.getName();
    private String streamId;
    private String mapType;
    private String contentType = S3Constants.DEFAULT_CONTENT_TYPE;
    private String bucketAcl = "";
    private int flushSize = -1;
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
                case S3Constants.AWS_REGION:
                    awsRegion = optionHolder.validateAndGetStaticValue(S3Constants.AWS_REGION);
                    break;
                case S3Constants.BUCKET_NAME:
                    bucketName = optionHolder.validateAndGetStaticValue(S3Constants.BUCKET_NAME);
                    break;
                case S3Constants.VERSIONING_ENABLED:
                    versioningEnabled = Boolean.parseBoolean(
                            optionHolder.validateAndGetStaticValue(S3Constants.VERSIONING_ENABLED));
                    break;
                case S3Constants.STORAGE_CLASS:
                    storageClass = optionHolder.validateAndGetStaticValue(S3Constants.STORAGE_CLASS);
                    break;
                case S3Constants.FLUSH_SIZE:
                    flushSize = Integer.parseInt(optionHolder.validateAndGetStaticValue(S3Constants.FLUSH_SIZE));
                    break;
                case S3Constants.CONTENT_TYPE:
                    contentType = optionHolder.validateAndGetStaticValue(S3Constants.CONTENT_TYPE);
                    break;
                case S3Constants.BUCKET_ACL:
                    bucketAcl = optionHolder.validateAndGetStaticValue(S3Constants.BUCKET_ACL);
                    break;
                default:
                    // Ignore! Not a valid option.
            }
        });

        if (bucketName == null || bucketName.isEmpty()) {
            throw new SiddhiAppCreationException("Parameter '" + S3Constants.BUCKET_NAME + "' is required.");
        }

        if (!optionHolder.isOptionExists(S3Constants.OBJECT_PATH)) {
            throw new SiddhiAppCreationException("Parameter '" + S3Constants.OBJECT_PATH + "' is required.");
        }

        if (flushSize <= 0) {
            flushSize = 1;
        }

        // Get stream id and the map type from stream definition.
        streamId = streamDefinition.getId();
        mapType = streamDefinition.getAnnotations().stream()
                .filter(e -> e.getName().equals("sink"))
                .findFirst()
                .get()
                .getAnnotations()
                .stream()
                .filter(e -> e.getName().equals("map"))
                .findFirst()
                .map(a -> a.getElement("type"))
                .orElseThrow(() -> new SiddhiAppCreationException("Sink mapper is required."));
    }

    public String getStorageClass() {
        return storageClass;
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

    public String getAwsRegion() {
        return awsRegion;
    }

    public String getStreamId() {
        return streamId;
    }

    public String getMapType() {
        return mapType;
    }

    public String getContentType() {
        return contentType;
    }

    public String getBucketAcl() {
        return bucketAcl;
    }

    public int getFlushSize() {
        return flushSize;
    }

    public boolean isVersioningEnabled() {
        return versioningEnabled;
    }
}
