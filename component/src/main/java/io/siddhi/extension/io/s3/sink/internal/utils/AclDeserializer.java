/*
 * Copyright (c) 2019, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.siddhi.extension.io.s3.sink.internal.utils;


import org.apache.log4j.Logger;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetBucketAclRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Grantee;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.Type;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code ACLDeserializer} de-serializes bucket ACL from ACL definition and generate list of {@code Grant}s.
 * The accepted ACL definition is as follows.
 * <p>
 * Ex: 'canonical:USER_UUID:FullControl,group:LoDelivery:Read,email:john@doe.com:Write,email:jane@doe.com:Read'
 */
public class AclDeserializer {

    private static final Logger logger = Logger.getLogger(AclDeserializer.class);

    private static final String CANONICAL_GRANTEE_TYPE = "canonical";
    private static final String GROUP_GRANTEE_TYPE = "group";
    private static final String EMAIL_GRANTEE_TYPE = "email";
    private static final String ACL_GROUP_DELIMITER = ",";
    private static final String ACL_GROUP_PARTS_DELIMITER = ":";

    private AclDeserializer() {
        // To prevent initialization of the class.
    }

    public static List<Grant> deserialize(String aclString) {
        List<Grant> grantList = new ArrayList<>();
        for (String grantString : aclString.split(ACL_GROUP_DELIMITER)) {
            String[] parts = grantString.split(ACL_GROUP_PARTS_DELIMITER);
            if (parts.length != 3) {
                continue;
            }
            Permission permission = getPermission(parts[2]);
            if (permission == null) {
                logger.warn("Invalid bucket permission '" + parts[2] + "' specified in grant " + grantString +
                        " in the bucket ACL. Possible values are FULL_CONTROL, READ, WRITE, READ_ACP, and WRITE_ACP.");
                continue;
            }
            Grantee grantee = null;
            switch (parts[0].toLowerCase()) {
                case CANONICAL_GRANTEE_TYPE: {
                    grantee = Grantee.builder().type(Type.CANONICAL_USER).id(parts[1]).build();
                    break;
                }
                case GROUP_GRANTEE_TYPE:
                    String uri = getGroupUri(parts[1]);
                    if (uri == null) {
                        logger.warn("Invalid group grantee '" + parts[1] + "' specified in grant " + grantString +
                                " in the bucket ACL. Possible values are AllUsers, AuthenticatedUsers, and " +
                                "LogDelivery.");
                        continue;
                    }
                    grantee = Grantee.builder().type(Type.GROUP).uri(uri).build();
                    break;
                case EMAIL_GRANTEE_TYPE:
                    grantee = Grantee.builder().type(Type.AMAZON_CUSTOMER_BY_EMAIL).emailAddress(parts[1]).build();
                    break;
                default:
                    // Not a valid grantee, hence ignoring.
                    logger.warn("Invalid grantee '" + parts[0] + "' specified in grant " + grantString +
                            " in the bucket ACL. Possible values are canonical, group, and email.");
                    break;
            }
            grantList.add(Grant.builder().grantee(grantee).permission(getPermission(parts[2])).build());
        }
        return grantList;
    }

    private static Permission getPermission(String permissionName) {
        for (Permission permission : Permission.values()) {
            if (permission.toString().equalsIgnoreCase(permissionName)) {
                return permission;
            }
        }
        return null;
    }

    private static String getGroupUri(String groupString) {
        switch (groupString.toLowerCase()) {
            case "logdelivery": {
                return "http://acs.amazonaws.com/groups/s3/LogDelivery";
            }
            case "authenticatedusers": {
                return "http://acs.amazonaws.com/groups/global/AuthenticatedUsers";
            }
            case "allusers": {
                return "http://acs.amazonaws.com/groups/global/AllUsers";
            }
        }
        return null;
    }
}
