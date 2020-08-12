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

package io.siddhi.extension.common.beans;

import io.siddhi.core.exception.SiddhiAppCreationException;
import io.siddhi.extension.io.s3.sink.internal.utils.S3Constants;
import software.amazon.awssdk.regions.Region;

import java.util.Map;

public class ClientConfig {

    private String credentialProviderClass = null;
    private String awsAccessKey = null;
    private String awsSecretKey = null;
    private Region awsRegion = Region.US_WEST_2;

    public ClientConfig() {
    }

    public static ClientConfig fromMap(Map<String, String> map) {
        ClientConfig config = new ClientConfig();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            switch (entry.getKey()) {
                case S3Constants.CREDENTIAL_PROVIDER_CLASS:
                    if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                        config.setCredentialProviderClass(entry.getValue());
                    }
                    break;
                case S3Constants.AWS_ACCESS_KEY:
                    if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                        config.setAwsAccessKey(entry.getValue());
                    }
                    break;
                case S3Constants.AWS_SECRET_KEY:
                    if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                        config.setAwsSecretKey(entry.getValue());
                    }
                    break;
                case S3Constants.AWS_REGION:
                    if (entry.getValue() != null && !entry.getValue().isEmpty()) {
                        config.setAwsRegion(Region.of(entry.getValue()));
                    }
                    break;
            }
        }
        return config;
    }

    public String getCredentialProviderClass() {
        return credentialProviderClass;
    }

    public void setCredentialProviderClass(String credentialProviderClass) {
        this.credentialProviderClass = credentialProviderClass;
    }

    public String getAwsAccessKey() {
        return awsAccessKey;
    }

    public void setAwsAccessKey(String awsAccessKey) {
        this.awsAccessKey = awsAccessKey;
    }

    public String getAwsSecretKey() {
        return awsSecretKey;
    }

    public void setAwsSecretKey(String awsSecretKey) {
        this.awsSecretKey = awsSecretKey;
    }

    public Region getAwsRegion() {
        return awsRegion;
    }

    public void setAwsRegion(Region awsRegion) {
        this.awsRegion = awsRegion;
    }

    public void validate() {
        boolean hasCredentialProviderClass = this.credentialProviderClass != null && !this.credentialProviderClass.isEmpty();
        boolean hasAccessKey = this.awsAccessKey != null && !this.awsAccessKey.isEmpty();
        boolean hasSecretKey = this.awsSecretKey != null && !this.awsSecretKey.isEmpty();

        if (hasCredentialProviderClass && (hasAccessKey || hasSecretKey)) {
            throw new SiddhiAppCreationException("Parameter '" + S3Constants.CREDENTIAL_PROVIDER_CLASS +
                    "' cannot be used along with '" + S3Constants.AWS_ACCESS_KEY + "' and/or '" +
                    S3Constants.AWS_SECRET_KEY + "'.");
        }

        if (!hasCredentialProviderClass && ((hasAccessKey && !hasSecretKey) || (!hasAccessKey && hasSecretKey))) {
            throw new SiddhiAppCreationException("Parameter '" + S3Constants.AWS_ACCESS_KEY +
                    "' should be used along with parameter '" + S3Constants.AWS_SECRET_KEY + "'.");
        }
    }
}
