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

package io.siddhi.extension.io.s3.sink.internal;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.BucketVersioningConfiguration;
import com.amazonaws.services.s3.model.CreateBucketRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SetBucketVersioningConfigurationRequest;
import io.siddhi.extension.io.s3.sink.internal.beans.EventObject;
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;
import io.siddhi.extension.io.s3.sink.internal.serializers.PayloadSerializer;
import io.siddhi.extension.io.s3.sink.internal.serializers.TextSerializer;
import io.siddhi.extension.io.s3.sink.internal.utils.ACLDeserializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;

public class ServiceClient {
    private SinkConfig config;
    private AmazonS3 client;
    private PayloadSerializer serializer;

    public ServiceClient(SinkConfig config) {
        this.config = config;
        this.client = buildClient();
        this.serializer = getPayloadSerializer();

        System.out.println(">>>>>>>>>>>>> init service client");

        // If the bucket is not available, create it.
        createBucketIfNotExist();
    }

    public void uploadObject(EventObject eventObject) {
        InputStream inputStream = serializer.serialize(eventObject);

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType(config.getContentType());
        try {
            metadata.setContentLength(inputStream.available());
        } catch (IOException e) {
            // Ignore setting content length
        }

        PutObjectRequest putObjectRequest = new PutObjectRequest(
                config.getBucketName(), buildKey(eventObject), inputStream, metadata);
        if (config.getStorageClass() != null && !config.getStorageClass().isEmpty()) {
            putObjectRequest.setStorageClass(config.getStorageClass());
        }
        client.putObject(putObjectRequest);
    }

    private AmazonS3 buildClient() {
        AmazonS3ClientBuilder builder = AmazonS3ClientBuilder.standard()
                .withRegion(config.getAwsRegion());
        AWSCredentialsProvider credentialProvider = getCredentialProvider();
        if (credentialProvider != null) {
            builder.withCredentials(credentialProvider);
        }
        return builder.build();
    }

    private AWSCredentialsProvider getCredentialProvider() {
        if (config.getCredentialProviderClass() != null) {
            try {
                return (AWSCredentialsProvider) this.getClass().getClassLoader().loadClass(config.getCredentialProviderClass()).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                e.printStackTrace();
            }
            return null;
        }

        if (config.getAwsAccessKey() != null && config.getAwsSecretKey() != null) {
            return new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(config.getAwsAccessKey(), config.getAwsSecretKey()));
        }
        return null;
    }

    private void createBucketIfNotExist() {
        System.out.println(">>>>>>>>>>> create bucket");

        // NOTE: The bucket.acl and versioning.enabled flags will only be effective if the bucket is not available.

        // Check if the bucket exists. If so skip the rest of the code.
        if (client.doesBucketExistV2(config.getBucketName())) {
            return;
        }

        // Create the bucket.
        CreateBucketRequest createBucketRequest = new CreateBucketRequest(
                config.getBucketName(), config.getAwsRegion());
        AccessControlList acl = buildBucketACL();
        if (acl != null) {
            createBucketRequest = createBucketRequest.withAccessControlList(acl);
        }
        client.createBucket(createBucketRequest);

        // Enable versioning only if the config flag is set.
        if (config.isVersioningEnabled()) {
            SetBucketVersioningConfigurationRequest bucketVersioningConfigurationRequest =
                    new SetBucketVersioningConfigurationRequest(config.getBucketName(),
                            new BucketVersioningConfiguration().withStatus(BucketVersioningConfiguration.ENABLED));
            client.setBucketVersioningConfiguration(bucketVersioningConfigurationRequest);
        }
    }

    private AccessControlList buildBucketACL() {
        List<Grant> grants = ACLDeserializer.deserialize(config.getBucketAcl());
        if (grants.size() > 0) {
            AccessControlList acl = new AccessControlList();
            acl.grantAllPermissions(grants.toArray(new Grant[0]));
            return acl;
        }
        return null;
    }

    private String buildKey(EventObject eventObject) {
        String key = String.format("%s-%s.%s", config.getStreamId(), eventObject.getObjectKeySuffix(),
                serializer.getExtension());
        return Paths.get(eventObject.getObjectPath(), key).toString();
    }

    private PayloadSerializer getPayloadSerializer() {
        ServiceLoader<PayloadSerializer> loader = ServiceLoader.load(PayloadSerializer.class);
        for (PayloadSerializer serializer : loader) {
            List<String> types = Arrays.asList(serializer.getTypes());
            if (types.contains(config.getMapType())) {
                return serializer;
            }
        }

        // If no serializer is found, use text serializer as default.
        return new TextSerializer();
    }
}
