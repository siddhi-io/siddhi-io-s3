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

package io.siddhi.extension.io.s3.sink.internal.strategies.countbased;

import io.siddhi.extension.io.s3.sink.internal.beans.EventObject;

public class CountBasedEventObject extends EventObject {
    private int offset;

    public CountBasedEventObject(String objectPath, int offset) {
        super(objectPath);
        this.offset = offset;
    }

    @Override
    public String toString() {
        return String.format("Event[offset=%d, path=%s, count=%s]", offset, objectPath, events.size());
    }

    @Override
    public String getObjectKeySuffix() {
        return Integer.toString(offset);
    }

    public int getOffset() {
        return offset;
    }
}
