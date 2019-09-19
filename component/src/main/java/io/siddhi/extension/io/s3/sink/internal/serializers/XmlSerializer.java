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

/**
 * {@code XmlSerializer} serializes the event payload into an XML.
 */
public class XmlSerializer extends PayloadSerializer {
    @Override
    public MapperTypes[] getTypes() {
        return new MapperTypes[]{MapperTypes.XML};
    }

    @Override
    public String getExtension() {
        return "xml";
    }

    @Override
    public InputStream serialize(EventObject eventObject) {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("<%s>%n", config.getXmlEnclosingElement()));
        for (Object event : eventObject.getEvents()) {
            sb.append(event).append(String.format("%n"));
        }
        sb.append(String.format("</%s>", config.getXmlEnclosingElement()));
        return serialize(sb.toString());
    }
}
