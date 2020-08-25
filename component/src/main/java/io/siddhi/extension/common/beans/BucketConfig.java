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
import io.siddhi.extension.common.utils.S3Constants;

import java.util.Map;

/**
 * Holds bucket configurations read from annotations
 */
public class BucketConfig {
    private String bucketName = null;
    private String bucketAcl = null;
    private boolean versioningEnabled = false;

    public BucketConfig() {
    }

    public static BucketConfig fromMap(Map<String, Object> map) {
        BucketConfig config = new BucketConfig();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            switch (entry.getKey()) {
                case S3Constants.BUCKET_NAME:
                    if (isValidString(entry.getValue())) {
                        config.setBucketName((String) entry.getValue());
                    }
                    break;
                case S3Constants.BUCKET_ACL:
                    if (isValidString(entry.getValue())) {
                        config.setBucketAcl((String) entry.getValue());
                    }
                    break;
                case S3Constants.VERSIONING_ENABLED:
                    if (isValidString(entry.getValue())) {
                        config.setVersioningEnabled(Boolean.parseBoolean((String) entry.getValue()));
                    }
                    break;
                default:
                    // Invalid config. Ignoring!
                    break;
            }
        }
        return config;
    }

    private static boolean isValidString(Object obj) {
        return obj != null && !((String) obj).isEmpty();
    }

    public String getBucketName() {
        return bucketName;
    }

    public void setBucketName(String bucketName) {
        this.bucketName = bucketName;
    }

    public String getBucketAcl() {
        return bucketAcl;
    }

    public void setBucketAcl(String bucketAcl) {
        this.bucketAcl = bucketAcl;
    }

    public boolean isVersioningEnabled() {
        return versioningEnabled;
    }

    public void setVersioningEnabled(boolean versioningEnabled) {
        this.versioningEnabled = versioningEnabled;
    }

    public void validate() {
        if (bucketName == null || bucketName.isEmpty()) {
            throw new SiddhiAppCreationException("Parameter '" + S3Constants.BUCKET_NAME + "' is required.");
        }
    }
}
