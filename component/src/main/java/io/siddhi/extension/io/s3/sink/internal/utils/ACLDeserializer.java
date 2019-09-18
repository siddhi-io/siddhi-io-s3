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

import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.EmailAddressGrantee;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.GroupGrantee;
import com.amazonaws.services.s3.model.Permission;

import java.util.ArrayList;
import java.util.List;

public class ACLDeserializer {

    private static final String CANONICAL_GRANTEE_TYPE = "canonical";
    private static final String GROUP_GRANTEE_TYPE = "group";
    private static final String EMAIL_GRANTEE_TYPE = "email";
    private static final String ACL_GROUP_DELIMITER = ",";
    private static final String ACL_GROUP_PARTS_DELIMITER = ":";

    private ACLDeserializer() {
        // To prevent initialization of the class.
    }

    public static List<Grant> deserialize(String aclString) {
        List<Grant> grantList = new ArrayList<>();
        for (String grantString : aclString.split(ACL_GROUP_DELIMITER)) {
            String[] parts = grantString.split(ACL_GROUP_PARTS_DELIMITER);
            if (parts.length != 3) {
                continue;
            }
            switch (parts[0].toLowerCase()) {
                case CANONICAL_GRANTEE_TYPE:
                    grantList.add(new Grant(new CanonicalGrantee(parts[1]), getPermission(parts[2])));
                    break;
                case GROUP_GRANTEE_TYPE:
                    grantList.add(new Grant(getGroupGrantee(parts[1]), getPermission(parts[2])));
                    break;
                case EMAIL_GRANTEE_TYPE:
                    grantList.add(new Grant(new EmailAddressGrantee(parts[1]), getPermission(parts[2])));
                    break;
            }
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

    private static GroupGrantee getGroupGrantee(String groupString) {
        for (GroupGrantee groupGrantee : GroupGrantee.values()) {
            if (groupGrantee.toString().equalsIgnoreCase(groupString)) {
                return groupGrantee;
            }
        }
        return null;
    }
}
