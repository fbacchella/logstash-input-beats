package org.logstash.beats;

import org.junit.Before;
import org.junit.Test;

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
        batch.addMessage(new Message(1, new HashMap<Object, Object>()));
        assertFalse(batch.isEmpty());
    }

    @Test
    public void testSize() {
        assertEquals(0, batch.size());
        batch.addMessage(new Message(1, new HashMap<Object, Object>()));
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
            batch.addMessage(new Message(i, new HashMap<Object, Object>()));
        }

        assertTrue(batch.isComplete());
    }

    @Test
    public void testCompleteReturnWhenTheNumberOfEventDoesntMatchBatchSize() {
        int numberOfEvent = 2;

        batch.setBatchSize(numberOfEvent);

        batch.addMessage(new Message(1, new HashMap<Object, Object>()));

        assertFalse(batch.isComplete());
    }

}
