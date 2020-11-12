package org.logstash.beats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;
import org.logstash.beats.BeatsParser.InvalidFrameProtocolException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.afterburner.AfterburnerModule;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

public class V2BatchTest {

    public static final ObjectMapper MAPPER = new ObjectMapper().registerModule(new AfterburnerModule());

    @Test
    public void testIsEmpty() throws InvalidFrameProtocolException {
        try (V2Batch batch = new V2Batch()) {
            assertTrue(batch.isEmpty());
            ByteBuf content = messageContents();
            batch.addMessage(1, content, content.readableBytes());
            assertFalse(batch.isEmpty());
        }
    }

    @Test
    public void testSize() throws InvalidFrameProtocolException {
        try (V2Batch batch = new V2Batch()) {
            assertEquals(0, batch.size());
            ByteBuf content = messageContents();
            batch.addMessage(1, content, content.readableBytes());
            assertEquals(1, batch.size());
        }
    }

    @Test
    public void testGetProtocol() {
        try (V2Batch batch = new V2Batch()) {
            assertEquals(Protocol.VERSION_2, batch.getProtocol());
        }
    }

    @Test
    public void testCompleteReturnTrueWhenIReceiveTheSameAmountOfEvent() throws InvalidFrameProtocolException {
        try (V2Batch batch = new V2Batch()) {
            int numberOfEvent = 2;
            batch.setBatchSize(numberOfEvent);
            for (int i = 1; i <= numberOfEvent; i++) {
                ByteBuf content = messageContents();
                batch.addMessage(i, content, content.readableBytes());
            }
            assertTrue(batch.isComplete());
        }
    }

    @Test
    public void testBigBatch() throws InvalidFrameProtocolException {
        try (V2Batch batch = new V2Batch()) {
            int size = 4096;
            assertEquals(0, batch.size());
            ByteBuf content = messageContents();
            for (int i = 0; i < size; i++) {
                batch.addMessage(i, content.asReadOnly(), content.readableBytes());
            }
            assertEquals(size, batch.size());
            int i = 0;
            for (Message message : batch) {
                assertEquals(message.getSequence(), i++);
            }
        }
    }

    @Test
    public void testHighSequence() throws InvalidFrameProtocolException {
        try (V2Batch batch = new V2Batch()) {
            int numberOfEvent = 2;
            int startSequenceNumber = new SecureRandom().nextInt(10000);
            batch.setBatchSize(numberOfEvent);
            ByteBuf content = messageContents();

            for (int i = 1; i <= numberOfEvent; i++) {
                batch.addMessage(startSequenceNumber + i, content, content.readableBytes());
            }

            assertEquals(startSequenceNumber + numberOfEvent, batch.getHighestSequence());
        }
    }


    @Test
    public void testOversizedBatch() throws InvalidFrameProtocolException {
        try (V2Batch batch = new V2Batch(2048)) {
            int size = 4096;
            assertEquals(0, batch.size());
            ByteBuf content = messageContents();
            InvalidFrameProtocolException ex = assertThrows(BeatsParser.InvalidFrameProtocolException.class, () -> {
                for (int i = 0; i < size; i++) {
                    batch.addMessage(i, content.asReadOnly(), content.readableBytes());
                }
            });
            assertThrows(BeatsParser.InvalidFrameProtocolException.class, () -> {
                batch.addMessage(70, content.asReadOnly(), content.readableBytes());
            });
            assertEquals("Oversized payload: 2070", ex.getMessage());
            assertEquals(89, batch.size());
            int i = 0;
            for (Message message : batch) {
                assertEquals(message.getSequence(), i++);
                Map<?, ?> data = message.getData();
                assertTrue(data.size() == 0 || data.size() == 1);
            }
        }
    }

    @Test
    public void testNoDataBatch() throws InvalidFrameProtocolException {
        try (V2Batch batch = new V2Batch(2048)) {
            assertEquals(0, batch.size());
            ByteBuf content = Unpooled.EMPTY_BUFFER;
            batch.addMessage(0, content.asReadOnly(), content.readableBytes());
            batch.addMessage(1, content.asReadOnly(), content.readableBytes());
            assertEquals(2, batch.size());
            int i = 0;
            for (Message message : batch) {
                assertEquals(message.getSequence(), i++);
                Map<?, ?> data = message.getData();
                assertTrue(data.size() == 0);
            }
        }
    }

    @Test
    public void testCompleteReturnWhenTheNumberOfEventDoesntMatchBatchSize() throws InvalidFrameProtocolException {
        try (V2Batch batch = new V2Batch()) {
            int numberOfEvent = 2;
            batch.setBatchSize(numberOfEvent);
            ByteBuf content = messageContents();
            batch.addMessage(1, content, content.readableBytes());
            assertFalse(batch.isComplete());
        }
    }

    public static ByteBuf messageContents() {
        Map<String, String> test = new HashMap<>();
        test.put("key", "value");
        try {
            byte[] bytes = MAPPER.writeValueAsBytes(test);
            return Unpooled.wrappedBuffer(bytes);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

    public static ByteBuf messageContents(int count) {
        Map<String, String> test = new HashMap<>(count);
        for (int i = 0 ; i < count ; i++) {
            test.put("key" + i, "value");
        }
        try {
            byte[] bytes = MAPPER.writeValueAsBytes(test);
            return Unpooled.wrappedBuffer(bytes);
        } catch (Exception e){
            throw new RuntimeException(e);
        }
    }

}
