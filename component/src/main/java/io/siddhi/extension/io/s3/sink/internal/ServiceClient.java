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

import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;
import io.siddhi.extension.io.s3.sink.internal.utils.MapperTypes;
import org.apache.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.VersioningConfiguration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * {@code ServiceClient} act as the proxy layer to work with the S3 endpoint.
 */
public class ServiceClient {
    private static final Logger logger = Logger.getLogger(ServiceClient.class);
    private static final String DEFAULT_CHARSET = "UTF-8";

    private SinkConfig config;
    private S3Client client;

    public ServiceClient(SinkConfig config) {
        this.config = config;
        this.client = buildClient();

        // If the bucket is not available, create it.
        createBucketIfNotExist();
    }

    public void uploadObject(String objectPath, Object payload, int offset) {
        logger.debug("Publishing the event to S3 at " + objectPath);
        InputStream inputStream = (payload instanceof ByteBuffer) ?
                new ByteArrayInputStream(((ByteBuffer) payload).array()) :
                new ByteArrayInputStream(((String) payload).getBytes(Charset.forName(DEFAULT_CHARSET)));

        PutObjectRequest.Builder putObjectBuilder = PutObjectRequest.builder()
                .bucket(config.getBucketName())
                .key(buildKey(objectPath, offset))
                .storageClass(config.getStorageClass());
        try {
            putObjectBuilder.contentLength((long) inputStream.available());
        } catch (IOException e) {
            // Ignore setting the content length
        }
        RequestBody requestBody = null;
        try {
            requestBody = RequestBody.fromInputStream(inputStream, inputStream.available());
        } catch (IOException e) {
            throw new SiddhiAppRuntimeException("Error while uploading the object", e);
        }
        client.putObject(putObjectBuilder.build(), requestBody);
    }

    private S3Client buildClient() {
        S3ClientBuilder builder = S3Client.builder()
                .region(config.getAwsRegion());
        AwsCredentialsProvider credentialsProvider = getCredentialProvider();
        if (credentialsProvider != null) {
            builder.credentialsProvider(credentialsProvider);
        }
        return builder.build();
    }

    private AwsCredentialsProvider getCredentialProvider() {
        if (config.getCredentialProviderClass() != null) {
            logger.debug("Authenticating user via the credential provider class.");
            try {
                /*return (AwsCredentialsProvider) this.getClass()
                        .getClassLoader()
                        .loadClass(config.getCredentialProviderClass())
                        .newInstance();*/
                Class credentialProviderClass = Class.forName(config.getCredentialProviderClass());
                return (AwsCredentialsProvider) credentialProviderClass.getDeclaredMethod("create")
                        .invoke(credentialProviderClass);
            } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new SiddhiAppCreationException("Error while authenticating the user.", e);
            } catch (ClassNotFoundException e) {
                throw new SiddhiAppCreationException("Unable to find the credential provider class " +
                        config.getCredentialProviderClass());
            }
        }

        if (config.getAwsAccessKey() != null && config.getAwsSecretKey() != null) {
            logger.debug("Authenticating the user via the access and secret keys.");
            AwsSessionCredentials awsCreds = AwsSessionCredentials.create(
                    config.getAwsAccessKey(),
                    config.getAwsAccessKey(),
                    "");
            return StaticCredentialsProvider.create(awsCreds);
        }
        logger.debug("No credential provider class or keys are provided. Hence falling back to default credential " +
                "provider chain.");
        return null;
    }

    private void createBucketIfNotExist() {
        // NOTE: The bucket.acl and versioning.enabled flags will only be effective if the bucket is not available.

        // Check if the bucket exists. If so skip the rest of the code.
        List<Bucket> buckets = client.listBuckets(ListBucketsRequest.builder().build()).buckets();
        int i = Collections.binarySearch(buckets, Bucket.builder().name(config.getBucketName()).build(),
                Comparator.comparing(Bucket::name));
        if (i >= 0) {
            return;
        }

        // Create the bucket.
        logger.debug("Bucket '" + config.getBucketName() + "' does not exist, hence creating.");
        CreateBucketRequest createBucketRequest = CreateBucketRequest
                .builder()
                .bucket(config.getBucketName())
                .createBucketConfiguration(CreateBucketConfiguration.builder()
                        .locationConstraint(config.getAwsRegion().id())
                        .build())
                .build();
        client.createBucket(createBucketRequest);

        // Enable versioning only if the config flag is set.
        if (config.isVersioningEnabled()) {
            client.putBucketVersioning(PutBucketVersioningRequest.builder()
                    .bucket(config.getBucketName())
                    .versioningConfiguration(VersioningConfiguration.builder().status(BucketVersioningStatus.ENABLED)
                            .build())
                    .build());
        }
    }

    /*private AccessControlList buildBucketACL() {
        String bucketAcl = config.getBucketAcl();
        if (bucketAcl == null || bucketAcl.isEmpty()) {
            return null;
        }
        List<Grant> grants = AclDeserializer.deserialize(bucketAcl);
        if (grants.size() > 0) {
            AccessControlList acl = new AccessControlList();
            acl.grantAllPermissions(grants.toArray(new Grant[0]));
            return acl;
        }
        return null;
    }*/

    private String buildKey(String objectPath, int offset) {
        String extension = MapperTypes.forName(config.getMapType()).getExtension();
        String key = (config.getNodeId() != null && !config.getNodeId().isEmpty()) ?
                String.format("%s-%s-%d.%s", config.getStreamId(), config.getNodeId(), offset, extension) :
                String.format("%s-%d.%s", config.getStreamId(), offset, extension);
        return Paths.get(objectPath, key).toString();
    }
}
