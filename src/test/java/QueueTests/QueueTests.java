package test.java.QueueTests;
//AWS SDK
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;
//TestNG
import org.testng.Assert;
import org.testng.annotations.*;
//RestAssured
import io.restassured.RestAssured;
import io.restassured.path.xml.XmlPath;
import io.restassured.http.Method;
import io.restassured.response.Response;
import io.restassured.specification.RequestSpecification;

import java.util.ArrayList;
import java.util.List;

public class QueueTests {

    private static String ENDPOINT = "http://localhost:9324";
    private static String region = "elasticmq";
    private static String accessKey = "x";
    private static String secretKey = "x";
    private static String queueName = "test_queue" ;
    private static Integer queueCount = 3;
    private static Integer msgCount = 3;
    private static AmazonSQS client;
    private static List<String> qurl = new ArrayList<>();
    private static List<String> msgId = new ArrayList<>();
    private static String queueUrl;

    @BeforeTest
    public void setUp() {
        client = AmazonSQSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(ENDPOINT, region)).build();

        // Create <queueCount> Queues
        for (int i = 1; i <= queueCount; i++) {

            CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName + i)
                    .addAttributesEntry("DelaySeconds", "0")
                    .addAttributesEntry("MessageRetentionPeriod", "86400");

            try {
                client.createQueue(createQueueRequest);
            } catch (AmazonSQSException e) {
                if (!e.getErrorCode().equals("QueueAlreadyExists")) {
                    throw e;
                }
            }
        }
        // List your queues
        ListQueuesResult lq_result = client.listQueues();
        System.out.println("Your SQS Queue URLs:");
        for (String url : lq_result.getQueueUrls()) {
            System.out.println("Queue URLs: " + url);
            qurl.add(url);
        }
    }
    @Test(priority = 0)
    public void CreateQueueTest() {
        // List Queues using REST HTTP Request
        RestAssured.baseURI = ENDPOINT;
        RequestSpecification httpRequest = RestAssured.given();
        // add action ListQueues
        Response response = httpRequest.request(Method.GET, "/?Action=ListQueues");
        String responseBody = response.getBody().asString();
        System.out.println("List Queues - Response Body: "+ responseBody);
        System.out.println("Queue URLs: " + response.xmlPath().get("//QueueUrl"));

        //Assert if response code is 200
        Assert.assertEquals(response.getStatusCode(),200);
        //Assert if the Queue size is equal to <queueCount>
        XmlPath cxml = new XmlPath(responseBody);
        Assert.assertEquals(cxml.get("ListQueuesResponse.ListQueuesResult.QueueUrl.size()"),queueCount);

    }
    @Test(priority = 1)
    public void SendMessageTest() {
        //Send <msgCount> messages to selected Queue
        queueUrl = client.getQueueUrl("test_queue1").getQueueUrl();

        for (int i = 1; i <= msgCount; i++) {
            SendMessageRequest send_msg_request = new SendMessageRequest()
                    .withQueueUrl(queueUrl)
                    .withMessageBody("test_message"+i)
                    .withDelaySeconds(0);
            msgId.add(client.sendMessage(send_msg_request).getMessageId());
            //test for successful send message - Assert MessageID not null
            Assert.assertNotNull(msgId);
        }
    }
    @Test(priority = 2)
    public void ReceiveMessageTest() {
        //Receive all messages from selected Queue
        List<Message> messages = client.receiveMessage(queueUrl).getMessages();

        //assert that all messages sent are received
        for (Message message : messages) {
           Assert.assertTrue(msgId.contains(message.getMessageId()));
        }
        // delete messages from the queue
        for (Message m : messages) {
            client.deleteMessage(queueUrl, m.getReceiptHandle());
        }
        //assert if all the messages are deleted
        Assert.assertTrue(client.receiveMessage(queueUrl).getMessages().isEmpty());
    }
    @AfterTest
    public void TearDown() {
        //Delete all the create queues
        for (String url : qurl) {
            client.deleteQueue(url);
        }
        //assert is queue is empty
        ListQueuesResult lq_result = client.listQueues();
        Assert.assertTrue(lq_result.getQueueUrls().isEmpty());
        qurl.clear();
    }
}