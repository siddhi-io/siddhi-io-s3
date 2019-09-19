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

package io.siddhi.extension.io.s3.sink.internal.serializers;

import io.siddhi.extension.io.s3.sink.internal.beans.EventObject;

import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * {@code BinarySerializer} serializes the event payload into a binary.
 */
public class BinarySerializer extends PayloadSerializer {
    @Override
    public MapperTypes[] getTypes() {
        return new MapperTypes[]{MapperTypes.Binary, MapperTypes.Avro};
    }

    @Override
    public String getExtension() {
        return "bin";
    }

    @Override
    public InputStream serialize(EventObject eventObject) {
        StringBuilder sb = new StringBuilder();
        for (Object event : eventObject.getEvents()) {
            sb.append(new String(((ByteBuffer) event).array(), DEFAULT_CHARSET))
                    .append(config.getBinaryDelimiter());
        }
        return serialize(sb.toString());
    }
}
