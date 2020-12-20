package org.logstash.beats;

import org.junit.Before;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.Collections;
import java.util.HashMap;

import static org.junit.Assert.*;

public class V1BatchTest {

    private V1Batch batch;

    @Before
    public void setUp() {
        batch = new V1Batch();
    }

    @Test
    public void testIsEmpty() {
        assertTrue(batch.isEmpty());
        batch.addMessage(new Message(1, new HashMap<String, Object>()));
        assertFalse(batch.isEmpty());
    }

    @Test
    public void testSize() {
        assertEquals(0, batch.size());
        batch.addMessage(new Message(1, new HashMap<String, Object>()));
        assertEquals(1, batch.size());
    }

    @Test
    public void testGetProtocol() {
        assertEquals(Protocol.VERSION_1, batch.getProtocol());
    }

    @Test
    public void testCompleteReturnTrueWhenIReceiveTheSameAmountOfEvent() {
        int numberOfEvent = 2;

        batch.setBatchSize(numberOfEvent);

        for (int i = 1; i <= numberOfEvent; i++) {
            batch.addMessage(new Message(i, new HashMap<String, Object>()));
        }

        assertTrue(batch.isComplete());
    }

    @Test
    public void testCompleteBatchWithSequenceNumbersNotStartingAtOne() {
        int numberOfEvent = 2;
        int startSequenceNumber = new SecureRandom().nextInt(10000);
        batch.setBatchSize(numberOfEvent);

        for (int i = 1; i <= numberOfEvent; i++) {
            batch.addMessage(new Message(startSequenceNumber + i, Collections.emptyMap()));
        }

        assertTrue(batch.isComplete());
    }

    @Test
    public void testHighSequence(){
        int numberOfEvent = 2;
        int startSequenceNumber = new SecureRandom().nextInt(10000);
        batch.setBatchSize(numberOfEvent);

        for (int i = 1; i <= numberOfEvent; i++) {
            batch.addMessage(new Message(startSequenceNumber + i, Collections.emptyMap()));
        }

        assertEquals(startSequenceNumber + numberOfEvent, batch.getHighestSequence());
    }

    @Test
    public void TestCompleteReturnWhenTheNumberOfEventDoesntMatchBatchSize() {
        int numberOfEvent = 2;

        batch.setBatchSize(numberOfEvent);

        batch.addMessage(new Message(1, new HashMap<String, Object>()));

        assertFalse(batch.isComplete());
    }

}
