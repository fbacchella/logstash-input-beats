package org.logstash.beats;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;

import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class BeatsParserTest {

    private V1Batch v1Batch;
    private V2Batch byteBufBatch;
    public static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new AfterburnerModule());

    private final int numberOfMessage = 20;

    @SuppressWarnings("deprecation")
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() throws Exception{
        v1Batch = new V1Batch();

        for (int i = 1; i <= numberOfMessage; i++) {
            Map<String, String> map = new HashMap<>();
            map.put("line", "Another world");
            map.put("from", "Little big Adventure");

            Message message = new Message(i, map);
            v1Batch.addMessage(message);
        }

        byteBufBatch = new V2Batch();

        for (int i = 1; i <= numberOfMessage; i++) {
            Map<String, String> map = new HashMap<>();
            map.put("line", "Another world");
            map.put("from", "Little big Adventure");
            ByteBuf bytebuf = Unpooled.wrappedBuffer(MAPPER.writeValueAsBytes(map));
            byteBufBatch.addMessage(i, bytebuf, bytebuf.readableBytes());
        }
    }

    @After
    public void tearDown() {
        Optional.ofNullable(byteBufBatch).ifPresent(V2Batch::release);
    }

    @Test
    public void testEncodingDecodingV1() {
        Batch decodedBatch = decodeBatch(v1Batch);
        assertMessages(v1Batch, decodedBatch);
        decodedBatch.release();
    }

    @Test
    public void testCompressedEncodingDecodingJson() {
        Batch decodedBatch = decodeCompressedBatch(byteBufBatch);
        assertMessages(byteBufBatch, decodedBatch);
        decodedBatch.release();
    }

    @Test
    public void testEncodingDecodingFields() {
        Batch decodedBatch = decodeBatch(v1Batch);
        assertMessages(v1Batch, decodedBatch);
        decodedBatch.release();
    }

    @Test
    public void testEncodingDecodingFieldWithUTFCharacters() throws Exception {
        V2Batch v2Batch = new V2Batch();

        // Generate Data with Keys and String with UTF-8
        for (int i = 0; i < numberOfMessage + 1; i++) {
            ByteBuf payload = Unpooled.buffer();

            Map<String, String> map = new HashMap<>();
            map.put("étoile", "mystère");
            map.put("from", "ÉeèAççï");

            byte[] json = MAPPER.writeValueAsBytes(map);
            payload.writeBytes(json);

            v2Batch.addMessage(i, payload, payload.readableBytes());
        }

        Batch decodedBatch = decodeBatch(v2Batch);
        assertMessages(v2Batch, decodedBatch);
        decodedBatch.release();
        v2Batch.release();
    }

    @Test
    public void testV1EncodingDecodingFieldWithUTFCharacters() {
        V1Batch batch = new V1Batch();

        // Generate Data with Keys and String with UTF-8
        for (int i = 0; i < numberOfMessage; i++) {

            Map<String, String> map = new HashMap<>();
            map.put("étoile", "mystère");
            map.put("from", "ÉeèAççï");

            Message message = new Message(i + 1, map);
            batch.addMessage(message);
        }

        Batch decodedBatch = decodeBatch(batch);
        assertMessages(batch, decodedBatch);
        decodedBatch.release();
    }

    @Test
    public void testCompressedEncodingDecodingFields() {
        Batch decodedBatch = decodeCompressedBatch(v1Batch);
        assertMessages(this.v1Batch, decodedBatch);
        decodedBatch.release();
    }

    @Test
    public void testOversizedFields() {
        thrown.expectCause(isA(BeatsParser.InvalidFrameProtocolException.class));

        Batch decodedBatch = decodeBatch(v1Batch, 9);
        assertMessages(v1Batch, decodedBatch);
        decodedBatch.release();
    }

    @Test
    public void testOversizedJson() {
        thrown.expectCause(isA(BeatsParser.InvalidFrameProtocolException.class));
        thrown.expectMessage("Oversized payload: 54");

        Batch decodedBatch = decodeBatch(byteBufBatch, 9);
        assertMessages(byteBufBatch, decodedBatch);
        decodedBatch.release();
    }

    @Test
    public void testShouldNotCrashOnGarbageData() {
        thrown.expectCause(isA(BeatsParser.InvalidFrameProtocolException.class));

        byte[] n = new byte[10000];
        new Random().nextBytes(n);
        ByteBuf randomBufferData = Unpooled.wrappedBuffer(n);

        sendPayloadToParser(randomBufferData);
    }

    @Test
    public void testOverflowJsonPayloadShouldRaiseAnException() throws JsonProcessingException {
        sendInvalidJSonPayload(4294967295L);
    }

    @Test
    public void testZeroSizeJsonPayloadShouldRaiseAnException() throws JsonProcessingException {
        sendInvalidJSonPayload(0);
    }

    @Test
    public void testOverflowFieldsCountShouldRaiseAnException() {
        sendInvalidV1Payload(Integer.MAX_VALUE + 1L);
    }

    @Test
    public void testZeroFieldsCountShouldRaiseAnException() {
        sendInvalidV1Payload(0);
    }

    @Test
    public void testUnsupportedVersionShouldRaiseAnException() {
        thrown.expectCause(isA(BeatsParser.InvalidFrameProtocolException.class));
        thrown.expectMessage("Unsupported protocol version: 3");

        ByteBuf payload = Unpooled.buffer();

        payload.writeByte(3);
        sendPayloadToParser(payload);
    }

    private void sendInvalidV1Payload(long size) {
        thrown.expectCause(isA(BeatsParser.InvalidFrameProtocolException.class));
        thrown.expectMessage("Invalid number of fields, received: " + size);

        ByteBuf payload = Unpooled.buffer();

        payload.writeByte(Protocol.VERSION_1);
        payload.writeByte(Protocol.CODE_WINDOW_SIZE);
        payload.writeInt(1);
        payload.writeByte(Protocol.VERSION_1);
        payload.writeByte(Protocol.CODE_FRAME);
        payload.writeInt(1);
        payload.writeInt((int)size);

        byte[] key = "message".getBytes();
        byte[] value = "Hola".getBytes();

        payload.writeInt(key.length);
        payload.writeBytes(key);
        payload.writeInt(value.length);
        payload.writeBytes(value);

        sendPayloadToParser(payload);
    }

    private void sendInvalidJSonPayload(long l) throws JsonProcessingException {
        thrown.expectCause(isA(BeatsParser.InvalidFrameProtocolException.class));
        thrown.expectMessage("Invalid json length, received: " + l);

        Map<String, String> mapData = Collections.singletonMap("message", "hola");

        ByteBuf payload = Unpooled.buffer();

        payload.writeByte(Protocol.VERSION_2);
        payload.writeByte(Protocol.CODE_WINDOW_SIZE);
        payload.writeInt(1);
        payload.writeByte(Protocol.VERSION_2);
        payload.writeByte(Protocol.CODE_JSON_FRAME);
        payload.writeInt(1);
        payload.writeInt((int)l);

        byte[] json = MAPPER.writeValueAsBytes(mapData);
        payload.writeBytes(json);

        sendPayloadToParser(payload);
    }

    private void sendPayloadToParser(ByteBuf payload) {
        EmbeddedChannel channel = new EmbeddedChannel(new BeatsParser());
        channel.writeOutbound(payload);
        Object o = channel.readOutbound();
        channel.writeInbound(o);
    }

    private void assertMessages(Batch expected, Batch actual) {

        assertNotNull(actual);
        assertEquals(expected.size(), actual.size());

        Iterator<Message> expectedMessages = expected.iterator();
        for (Message actualMessage: actual) {
            Message expectedMessage = expectedMessages.next();
            assertEquals(expectedMessage.getSequence(), actualMessage.getSequence());

            Map<?, ?> expectedData = expectedMessage.getData();
            Map<?, ?> actualData = actualMessage.getData();

            assertEquals(expectedData.size(), actualData.size());

            for (Map.Entry<?, ?> e : expectedData.entrySet()) {
                String key = (String) e.getKey();
                String value = (String) e.getValue();

                assertEquals(value, actualData.get(key));
            }
        }
    }

    private Batch decodeCompressedBatch(Batch batch) {
        EmbeddedChannel channel = new EmbeddedChannel(new CompressedBatchEncoder(), new BeatsParser());
        channel.writeOutbound(batch);
        Object o = channel.readOutbound();
        channel.writeInbound(o);

        return (Batch) channel.readInbound();
    }

    private Batch decodeBatch(Batch batch) {
        return decodeBatch(batch, -1);
    }

    private Batch decodeBatch(Batch batch, int maxPayloadSize) {
        EmbeddedChannel channel = new EmbeddedChannel(new BatchEncoder(), new BeatsParser(maxPayloadSize));
        channel.writeOutbound(batch);
        Object o = channel.readOutbound();
        channel.writeInbound(o);

        return (Batch) channel.readInbound();
    }

}
