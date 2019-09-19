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
import io.siddhi.extension.io.s3.sink.internal.beans.SinkConfig;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.Charset;

/**
 * The interface for serializers which can be used to serialize event payloads.
 */
public abstract class PayloadSerializer implements Serializable {

    protected static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    protected SinkConfig config;

    public abstract MapperTypes[] getTypes();

    public abstract String getExtension();

    public abstract InputStream serialize(EventObject eventObject);

    public void setConfig(SinkConfig config) {
        this.config = config;
    }

    protected InputStream serialize(String string) {
        return new ByteArrayInputStream(string.getBytes(DEFAULT_CHARSET));
    }
}
