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

package io.siddhi.extension.common;

import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.core.exception.SiddhiAppRuntimeException;
import io.siddhi.extension.common.beans.BucketConfig;
import io.siddhi.extension.common.beans.ClientConfig;
import io.siddhi.extension.common.utils.AclDeserializer;
import org.apache.log4j.Logger;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.AwsSessionCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.AccessControlPolicy;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.BucketVersioningStatus;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.CreateBucketConfiguration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.PutBucketAclRequest;
import software.amazon.awssdk.services.s3.model.PutBucketVersioningRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.s3.model.Type;
import software.amazon.awssdk.services.s3.model.VersioningConfiguration;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * {@code S3ServiceClient} act as the proxy layer to work with the S3 endpoint.
 */
public class S3ServiceClient {
    private static final Logger logger = Logger.getLogger(S3ServiceClient.class);
    private static final String DEFAULT_CHARSET = "UTF-8";

    private S3Client client;
    private ClientConfig clientConfig;

    public S3ServiceClient(ClientConfig clientConfig) {
        this.clientConfig = clientConfig;
        this.client = buildClient();
    }

    public void ensureBucketAvailability(BucketConfig bucketConfig) {
        // NOTE: The bucket.acl and versioning.enabled flags will only be effective if the bucket is not available.

        // Check if the bucket exists. If so skip the rest of the code.
        List<Bucket> buckets;
        try {
            buckets = client.listBuckets(ListBucketsRequest.builder().build()).buckets();
        } catch (SdkClientException e) {
            throw new SiddhiAppCreationException("Invalid region id provided, given region id: '" +
                    clientConfig.getAwsRegion() + "'.", e);
        } catch (S3Exception e) {
            throw new SiddhiAppCreationException("Invalid credential provided, check your user credential class or" +
                    " access-key and secret-key.", e);
        }
        int i = Collections.binarySearch(buckets, Bucket.builder().name(bucketConfig.getBucketName()).build(),
                Comparator.comparing(Bucket::name));
        if (i >= 0) {
            return;
        }

        // Create the bucket.
        logger.debug("Bucket '" + bucketConfig.getBucketName() + "' does not exist, hence creating.");
        CreateBucketRequest createBucketRequest = CreateBucketRequest
                .builder()
                .bucket(bucketConfig.getBucketName())
                .createBucketConfiguration(CreateBucketConfiguration.builder()
                        .locationConstraint(clientConfig.getAwsRegion().id())
                        .build())
                .build();
        client.createBucket(createBucketRequest);

        // Enable versioning only if the config flag is set.
        if (bucketConfig.isVersioningEnabled()) {
            client.putBucketVersioning(PutBucketVersioningRequest.builder()
                    .bucket(bucketConfig.getBucketName())
                    .versioningConfiguration(VersioningConfiguration.builder().status(BucketVersioningStatus.ENABLED)
                            .build())
                    .build());
        }
        //add ACL permissions if "bucket.acl" flag is set
        String bucketAcl = bucketConfig.getBucketAcl();
        if (bucketAcl == null || bucketAcl.isEmpty()) {
            return;
        }
        List<Grant> grants = AclDeserializer.deserialize(bucketAcl);
        addACLPermissions(client, bucketConfig.getBucketName(),
                getOwnerCanonicalId(client, bucketConfig.getBucketName()), grants);
    }
    
    public void uploadObject(String bucketName, String key, Path path, StorageClass storageClass) {
        this.uploadObject(bucketName, key, path, null, storageClass);
    }

    public void uploadObject(String bucketName, String key, Object obj, String contentType, StorageClass storageClass) {
        PutObjectRequest.Builder builder = PutObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .storageClass(storageClass);

        if (contentType != null) {
            builder.contentType(contentType);
        }

        RequestBody requestBody;
        if (obj instanceof Path) {
            requestBody = RequestBody.fromFile((Path) obj);
        } else {
            InputStream inputStream = obj instanceof ByteBuffer ?
                    new ByteArrayInputStream(((ByteBuffer) obj).array()) :
                    new ByteArrayInputStream(((String) obj).getBytes(Charset.forName(DEFAULT_CHARSET)));
            try {
                builder.contentLength((long) inputStream.available());
            } catch (IOException e) {
                // Ignore setting the content length
            }
            try {
                requestBody = RequestBody.fromInputStream(inputStream, inputStream.available());
            } catch (IOException e) {
                throw new SiddhiAppRuntimeException("Error while uploading the object", e);
            }
        }
        client.putObject(builder.build(), requestBody);
    }

    public void copyObject(String srcBucketName, String srcKey, String destBucketName, String destKey,
                           StorageClass storageClass) {
        String encodedUrl;
        try {
            encodedUrl = URLEncoder.encode(Paths.get(srcBucketName, srcKey).toString(),
                    StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException e) {
            throw new SiddhiAppRuntimeException("Cannot encode the source URL", e);
        }
        client.copyObject(CopyObjectRequest.builder()
                .copySource(encodedUrl)
                .bucket(destBucketName)
                .key(destKey)
                .storageClass(storageClass)
                .build());
    }

    public void deleteObject(String bucketName, String key) {
        client.deleteObject(DeleteObjectRequest.builder()
                .bucket(bucketName)
                .key(key)
                .build());
    }

    private S3Client buildClient() {
        SdkHttpClient httpClient = ApacheHttpClient.builder().build();
        S3ClientBuilder builder = S3Client.builder()
                .region(clientConfig.getAwsRegion());
        AwsCredentialsProvider credentialsProvider = getCredentialProvider();
        if (credentialsProvider != null) {
            builder.credentialsProvider(credentialsProvider);
        }
        return builder.httpClient(httpClient).build();
    }

    private AwsCredentialsProvider getCredentialProvider() {
        if (clientConfig.getCredentialProviderClass() != null) {
            logger.debug("Authenticating user via the credential provider class.");
            try {
                Class credentialProviderClass = Class.forName(clientConfig.getCredentialProviderClass());
                return (AwsCredentialsProvider) credentialProviderClass.getDeclaredMethod("create")
                        .invoke(credentialProviderClass);
            } catch (IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new SiddhiAppCreationException("Error while authenticating the user.", e);
            } catch (ClassNotFoundException e) {
                throw new SiddhiAppCreationException("Unable to find the credential provider class " +
                        clientConfig.getCredentialProviderClass());
            }
        }

        if (clientConfig.getAwsAccessKey() != null && clientConfig.getAwsSecretKey() != null) {
            logger.debug("Authenticating the user via the access and secret keys.");
            AwsSessionCredentials awsCreds = AwsSessionCredentials.create(
                    clientConfig.getAwsAccessKey(),
                    clientConfig.getAwsSecretKey(),
                    "");
            return StaticCredentialsProvider.create(awsCreds);
        }
        logger.debug("No credential provider class or keys are provided. Hence falling back to default credential " +
                "provider chain.");
        return null;
    }

    private void addACLPermissions(S3Client s3, String bucketName, String ownerCanonicalId, List<Grant> grantList) {

        Grant ownerGrant = Grant.builder()
                .grantee(builder -> {
                    builder.id(ownerCanonicalId)
                            .type(Type.CANONICAL_USER);
                })
                .permission(Permission.FULL_CONTROL)
                .build();
        grantList.add(ownerGrant);
        AccessControlPolicy acl = AccessControlPolicy.builder()
                .owner(builder -> builder.id(ownerCanonicalId))
                .grants(grantList)
                .build();
        //put the new acl
        PutBucketAclRequest putAclReq = PutBucketAclRequest.builder()
                .bucket(bucketName)
                .accessControlPolicy(acl)
                .build();
        try {
            s3.putBucketAcl(putAclReq);
        } catch (S3Exception e) {
            logger.error("Error while adding ACL permission to the bucket ", e);
        }
    }

    private String getOwnerCanonicalId(S3Client s3, String bucketName) {
        GetBucketAclRequest bucketAclReq = GetBucketAclRequest.builder()
                .bucket(bucketName)
                .build();
        GetBucketAclResponse getAclRes = s3.getBucketAcl(bucketAclReq);
        return getAclRes.owner().id();
    }
}
