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

package io.siddhi.extension.io.s3.sink.internal.publisher;

import io.siddhi.extension.io.s3.sink.internal.beans.EventObject;
import io.siddhi.extension.io.s3.sink.internal.ServiceClient;
import org.apache.log4j.Logger;

public class PublisherTask implements Runnable {
    private static final Logger logger = Logger.getLogger(PublisherTask.class);

    private final ServiceClient client;
    private EventObject eventObject;

    public PublisherTask(EventObject eventObject, ServiceClient client) {
        this.eventObject = eventObject;
        this.client = client;
    }

    @Override
    public void run() {
        logger.debug("Publishing event object: " + eventObject);
        client.uploadObject(eventObject);
    }
}
