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

package io.siddhi.extension.io.s3.sink;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.input.InputHandler;
import org.testng.Assert;
import org.testng.annotations.Test;

public class S3SinkTestCase {
    private static final String BUCKET_NAME = "siddhi-io-s3-test-bucket";

    // Before running this test provide valid credentials and bucket details.
    // Due to not having a service to test against this, the test is commented out in the testng.xml
    @Test
    public void sinkTest1() throws InterruptedException {
        String streams = "" +
                "define window BarWindow(name string, age int) lengthBatch(3) output all events;\";\n" +
                "define stream FooStream(name string, age int);\n" +
                "\n" +
                "@sink(type='s3', bucket.name='" + BUCKET_NAME + "',object.path='test/users', " +
                "credential.provider='com.amazonaws.auth.profile.ProfileCredentialsProvider', node.id='zeus', \n" +
                "    @map(type='json', enclosing.element='$.user', \n" +
                "        @payload(\"\"\"{\"name\": \"{{name}}\", \"age\": {{age}}}\"\"\"))) \n" +
                "define stream BarStream(name string, age int);";
        String query = "" +
                "from FooStream\n" +
                "insert into BarWindow;\n" +
                "\n" +
                "from BarWindow\n" +
                "select name, age\n" +
                "insert into BarStream;";

        SiddhiManager siddhiManager = new SiddhiManager();
        SiddhiAppRuntime siddhiAppRuntime = siddhiManager.createSiddhiAppRuntime(streams + query);
        InputHandler fooStream = siddhiAppRuntime.getInputHandler("FooStream");

        siddhiAppRuntime.start();

        fooStream.send(new Object[]{"Ann", 22});
        fooStream.send(new Object[]{"Bob", 25});
        fooStream.send(new Object[]{"Charlie", 22});
        fooStream.send(new Object[]{"David", 23});
        fooStream.send(new Object[]{"Ellis", 25});
        fooStream.send(new Object[]{"Frank", 24});
        fooStream.send(new Object[]{"Greg", 22});

        Thread.sleep(2000);

        ListObjectsV2Request request = new ListObjectsV2Request().withBucketName(BUCKET_NAME).withMaxKeys(10);
        ListObjectsV2Result objects = getClient().listObjectsV2(request);

        Assert.assertEquals(objects.getObjectSummaries().size(), 2);
        siddhiAppRuntime.shutdown();
    }

    private AmazonS3 getClient() {
        return AmazonS3ClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(Regions.DEFAULT_REGION)
                .build();
    }
}
