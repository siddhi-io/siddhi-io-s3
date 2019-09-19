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
    private static final String BUCKET_NAME = "s3test-bucket";

    // Before running this test provide valid credentials and bucket details.
    // Due to not having a service to test against this, the test is commented out in the testng.xml
    @Test (enabled = false)
    public void sinkTest1() throws InterruptedException {
        String streams = "define stream FooStream(name string, age int);" +
                "@sink(type='s3', bucket.name='" + BUCKET_NAME + "', object.path='foo/users/', " +
                "credential.provider='com.amazonaws.auth.profile.ProfileCredentialsProvider', flush.size='3', " +
                "    @map(type='json', enclosing.element='$.user', " +
                "        @payload(\"\"\"{\"name\": \"{{name}}\", \"age\": {{age}}}\"\"\"))) " +
                "define stream BarStream(name string, age int);";
        String query = "from FooStream\n" +
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
