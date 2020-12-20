package org.logstash.beats;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of {@link Batch} intended for batches constructed from v1 protocol
 *
 */
public class V1Batch implements Batch {

    private int batchSize = 0;
    private List<Message> messages = new ArrayList<>();
    private int highestSequence = -1;

    @Override
    public byte getProtocol() {
        return Protocol.VERSION_1;
    }

    /**
     * Add Message to the batch
     * @param message Message to add to the batch
     */
    void addMessage(Message message) {
        message.setBatch(this);
        messages.add(message);
        if (message.getSequence() > highestSequence) {
            highestSequence = message.getSequence();
        }
    }

    @Override
    public Iterator<Message> iterator() {
        return messages.iterator();
    }

    @Override
    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    @Override
    public int size() {
        return messages.size();
    }

    @Override
    public boolean isEmpty() {
        return messages.isEmpty();
    }

    @Override
    public int getHighestSequence() {
        return highestSequence;
    }

    @Override
    public void release() {
        messages.clear();
        highestSequence = -1;
        batchSize = 0;
    }

}
