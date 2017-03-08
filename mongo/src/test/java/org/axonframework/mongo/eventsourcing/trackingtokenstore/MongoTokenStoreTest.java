package org.axonframework.mongo.eventsourcing.trackingtokenstore;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import org.axonframework.eventhandling.tokenstore.TokenStore;
import org.axonframework.eventhandling.tokenstore.UnableToClaimTokenException;
import org.axonframework.eventsourcing.eventstore.TrackingToken;
import org.axonframework.mongo.utils.MongoLauncher;
import org.axonframework.serialization.Serializer;
import org.axonframework.serialization.SimpleSerializedObject;
import org.axonframework.serialization.SimpleSerializedType;
import org.axonframework.serialization.xml.XStreamSerializer;
import org.bson.Document;
import org.bson.types.Binary;
import org.junit.*;
import org.junit.runner.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalAmount;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:META-INF/spring/mongo-context.xml"})
public class MongoTokenStoreTest {

    private TokenStore testSubject;

    private static MongodExecutable mongoExe;
    private static MongodProcess mongod;

    private MongoCollection<Document> trackingTokensCollection;
    private Serializer serializer;
    private TemporalAmount claimTimeout = Duration.ofSeconds(5);
    private Class<byte[]> contentType = byte[].class;

    @Autowired
    private ApplicationContext context;

    @BeforeClass
    public static void startMongoDB() throws Exception {
        mongoExe = MongoLauncher.prepareExecutable();
        mongod = mongoExe.start();
    }

    @AfterClass
    public static void stopMongoDB() throws Exception {
        if (mongod != null) {
            mongod.stop();
        }
        if (mongoExe != null) {
            mongoExe.stop();
        }
    }

    @Before
    public void setUp() throws Exception {
        MongoClient mongoClient = context.getBean(MongoClient.class);
        serializer = new XStreamSerializer();

        MongoTemplate mongoTemplate = new DefaultMongoTemplate(mongoClient);
        trackingTokensCollection = mongoTemplate.trackingTokensCollection();
        testSubject = new MongoTokenStore(mongoTemplate,
                                          serializer,
                                          claimTimeout,
                                          "testOwner",
                                          contentType);
    }

    @After
    public void tearDown() throws Exception {
        trackingTokensCollection.drop();
    }

    @Test
    public void testStoreToken_CreateTokenEntryIfItDoesNotExist() throws Exception {
        testStoreToken(null);
    }

    @Test
    public void testStoreToken_UpdateExistingTokenEntry() throws Exception {
        long timestamp = Instant.now().toEpochMilli();
        Document tokenEntry = new Document("processorName", "testProcessorName")
                .append("segment", 10)
                .append("owner", "testOwner")
                .append("timestamp", timestamp);
        trackingTokensCollection.insertOne(tokenEntry);

        testStoreToken(timestamp);
    }

    @Test(expected = UnableToClaimTokenException.class)
    public void testStoreToken_AlreadyClaimedToken() throws Exception {
        Document tokenEntry = new Document("processorName", "testProcessorName")
                .append("segment", 10)
                .append("owner", "anotherOwner")
                .append("timestamp", Instant.now().toEpochMilli());
        trackingTokensCollection.insertOne(tokenEntry);

        testStoreToken(null);
    }

    @Test
    public void testStoreToken_TimedOutClaim() throws Exception {
        Document tokenEntry = new Document("processorName", "testProcessorName")
                .append("segment", 10)
                .append("owner", "anotherOwner")
                .append("timestamp", Instant.now().minus(claimTimeout).toEpochMilli());
        trackingTokensCollection.insertOne(tokenEntry);

        testStoreToken(null);
    }

    private void testStoreToken(Long oldTimestamp) throws Exception {
        // Small delay to make sure the timestamp is updated if an existing token entry is updated.
        Thread.sleep(1);

        StubTrackingToken trackingToken = new StubTrackingToken("test");
        testSubject.storeToken(trackingToken, "testProcessorName", 10);

        MongoCursor<Document> documents = trackingTokensCollection.find()
                                                                  .iterator();
        Assert.assertTrue(documents.hasNext());

        Document document = documents.next();
        Assert.assertEquals("testProcessorName", document.getString("processorName"));
        Assert.assertEquals(Integer.valueOf(10), document.getInteger("segment"));
        Assert.assertEquals("testOwner", document.getString("owner"));
        Long timestamp = document.getLong("timestamp");

        if (oldTimestamp != null) {
            Assert.assertNotEquals(timestamp, oldTimestamp);
        }

        Binary token = document.get("token", Binary.class);
        Assert.assertNotNull(token);

        byte[] serializedTrackingToken = token.getData();
        SimpleSerializedType serializedType = new SimpleSerializedType(StubTrackingToken.class.getTypeName(), null);
        StubTrackingToken deserializedTrackingToken = serializer.deserialize(new SimpleSerializedObject<>(
                serializedTrackingToken,
                contentType,
                serializedType));

        Assert.assertTrue(trackingToken.equals(deserializedTrackingToken));

        Assert.assertFalse(documents.hasNext());
    }

    @Test
    public void testFetchToken() throws Exception {

    }

    @Test
    public void testExtendClaim() throws Exception {

    }

    @Test
    public void testReleaseClaim() throws Exception {

    }

    private static class StubTrackingToken implements TrackingToken {

        private final String value;

        public StubTrackingToken(String value) {
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            StubTrackingToken that = (StubTrackingToken) o;

            return value.equals(that.value);
        }
    }
}