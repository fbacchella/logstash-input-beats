package org.logstash.beats;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.logstash.beats.BeatsParser.InvalidFrameProtocolException;

import com.fasterxml.jackson.databind.ObjectReader;

import io.netty.buffer.ByteBuf;

/**
 * Implementation of {@link Batch} for the v2 protocol backed by ByteBuf. *must* be released after use.
 */
public class V2Batch implements Batch, Closeable {

    private int batchSize = 0;
    private final int maxPayloadSize;
    private int batchBytes = 0;
    private int highestSequence = -1;
    private final ObjectReader jsonReader;
    private final List<Message> messages = new ArrayList<>();

    public V2Batch() {
        maxPayloadSize = Integer.MAX_VALUE;
        jsonReader = DefaultJson.get();
    }

    public V2Batch(int maxPayloadSize) {
        this.maxPayloadSize = maxPayloadSize < 0 ? Integer.MAX_VALUE : maxPayloadSize;
        jsonReader = DefaultJson.get();
    }

    public V2Batch(int maxPayloadSize, ObjectReader jsonReader) {
        this.maxPayloadSize = maxPayloadSize < 0 ? Integer.MAX_VALUE : maxPayloadSize;
        this.jsonReader = jsonReader;
    }

    @Override
    public byte getProtocol() {
        return Protocol.VERSION_2;
    }

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

    /**
     * Adds a message to the batch, which will be constructed into an actual {@link Message} lazily.
     * @param sequenceNumber sequence number of the message within the batch
     * @param buffer A ByteBuf pointing to serialized JSon
     * @param size size of the serialized Json
     * @throws InvalidFrameProtocolException 
     */
    void addMessage(int sequenceNumber, ByteBuf buffer, int size) throws InvalidFrameProtocolException {
        if ((size + batchBytes) > maxPayloadSize) {
            batchSize = messages.size();
            throw new InvalidFrameProtocolException("Oversized payload: " + (size + batchBytes));
        }
        Message message = new Message(sequenceNumber, buffer.readSlice(size), jsonReader);
        message.setBatch(V2Batch.this);
        messages.add(message);
        batchBytes += size;
        if (sequenceNumber > highestSequence) {
            highestSequence = sequenceNumber;
        }
    }

    @Override
    public void release() {
        messages.clear();
        batchBytes = 0;
        highestSequence = -1;
        batchSize = 0;
    }

}
