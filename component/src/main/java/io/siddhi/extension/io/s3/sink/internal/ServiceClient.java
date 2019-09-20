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
import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;
import io.siddhi.extension.io.s3.sink.internal.utils.AclDeserializer;
import io.siddhi.extension.io.s3.sink.internal.utils.MapperTypes;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.List;

/**
 * {@code ServiceClient} act as the proxy layer to work with the S3 endpoint.
 */
public class ServiceClient {
    private static final Logger logger = Logger.getLogger(ServiceClient.class);
    private static final String DEFAULT_CHARSET = "UTF-8";

    private SinkConfig config;
    private AmazonS3 client;

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

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType(config.getContentType());
        try {
            metadata.setContentLength(inputStream.available());
        } catch (IOException e) {
            // Ignore setting the content length
        }

        PutObjectRequest request = new PutObjectRequest(config.getBucketName(), buildKey(objectPath, offset),
                inputStream, metadata);
        request.setStorageClass(config.getStorageClass().toString());
        client.putObject(request);
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
            logger.debug("Authenticating user via the credential provider class.");
            try {
                return (AWSCredentialsProvider) this.getClass()
                        .getClassLoader()
                        .loadClass(config.getCredentialProviderClass())
                        .newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new SiddhiAppCreationException("Error while authenticating the user.", e);
            } catch (ClassNotFoundException e) {
                throw new SiddhiAppCreationException("Unable to find the credential provider class " +
                        config.getCredentialProviderClass());
            }
        }

        if (config.getAwsAccessKey() != null && config.getAwsSecretKey() != null) {
            logger.debug("Authenticating the user via the access and secret keys.");
            return new AWSStaticCredentialsProvider(
                    new BasicAWSCredentials(config.getAwsAccessKey(), config.getAwsSecretKey()));
        }
        logger.debug("No credential provider class or keys are provided. Hence falling back to default credential " +
                "provider chain.");
        return null;
    }

    private void createBucketIfNotExist() {
        // NOTE: The bucket.acl and versioning.enabled flags will only be effective if the bucket is not available.

        // Check if the bucket exists. If so skip the rest of the code.
        if (client.doesBucketExistV2(config.getBucketName())) {
            logger.debug("Bucket '" + config.getBucketName() + "' is already exists." );
            return;
        }

        // Create the bucket.
        logger.debug("Bucket '" + config.getBucketName() + "' does not exist, hence creating." );

        CreateBucketRequest createBucketRequest = new CreateBucketRequest(
                config.getBucketName(), config.getAwsRegion().getName());
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
        List<Grant> grants = AclDeserializer.deserialize(config.getBucketAcl());
        if (grants.size() > 0) {
            AccessControlList acl = new AccessControlList();
            acl.grantAllPermissions(grants.toArray(new Grant[0]));
            return acl;
        }
        return null;
    }

    private String buildKey(String objectPath, int offset) {
        String extension = MapperTypes.forName(config.getMapType()).getExtension();
        String key = (config.getNodeId() != null && !config.getNodeId().isEmpty()) ?
                String.format("%s-%s-%d.%s", config.getStreamId(), config.getNodeId(), offset, extension) :
                String.format("%s-%d.%s", config.getStreamId(), offset, extension);
        return Paths.get(objectPath, key).toString();
    }
}
