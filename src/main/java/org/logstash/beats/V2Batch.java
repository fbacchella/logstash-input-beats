package org.logstash.beats;

import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.logstash.beats.BeatsParser.InvalidFrameProtocolException;

import com.fasterxml.jackson.databind.ObjectReader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * Implementation of {@link Batch} for the v2 protocol backed by ByteBuf. *must* be released after use.
 */
public class V2Batch implements Batch, Closeable {

    private class MessageIterator implements Iterator<Message> {
        private int read = 0;
        private final ByteBuf readerBuffer = internalBuffer.asReadOnly().resetReaderIndex();
        @Override
        public boolean hasNext() {
            return read < written;
        }
        @Override
        public Message next() {
            if (read >= written) {
                throw new NoSuchElementException();
            }
            int sequenceNumber = readerBuffer.readInt();
            int readableBytes = readerBuffer.readInt();
            Message message = new Message(sequenceNumber, readerBuffer.slice(readerBuffer.readerIndex(), readableBytes), jsonReader);
            readerBuffer.readerIndex(readerBuffer.readerIndex() + readableBytes);
            message.setBatch(V2Batch.this);
            message.getData();
            read++;
            return message;
        }
    }

    private ByteBuf internalBuffer = PooledByteBufAllocator.DEFAULT.buffer();
    private int written = 0;
    private static final int SIZE_OF_INT = 4;
    private int batchSize;
    private final int maxPayloadSize;
    private int highestSequence = -1;
    private final ObjectReader jsonReader;

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
        return new MessageIterator();
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
        return written;
    }

    @Override
    public boolean isEmpty() {
        return written == 0;
    }

    @Override
    public boolean isComplete() {
        return written == batchSize;
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
        if ((internalBuffer.readableBytes() + size + (2 * SIZE_OF_INT)) > maxPayloadSize) {
            batchSize = written;
            throw new InvalidFrameProtocolException("Oversized payload: " + (internalBuffer.readableBytes() + size + (2 * SIZE_OF_INT)));
        }
        written++;
        if (internalBuffer.writableBytes() < size + (2 * SIZE_OF_INT)) {
            // increase size slightly faster than what is really need,
            // to avoid doing an alloc for each message
            int newSize = (internalBuffer.capacity() + size + (2 * SIZE_OF_INT));
            int effectiveNewSize = Math.min((int)(newSize * 1.5), maxPayloadSize);
            internalBuffer.capacity(effectiveNewSize);
        }
        internalBuffer.writeInt(sequenceNumber);
        internalBuffer.writeInt(size);
        buffer.readBytes(internalBuffer, size);
        if (sequenceNumber > highestSequence) {
            highestSequence = sequenceNumber;
        }
    }

    @Override
    public void release() {
        if (internalBuffer != null) {
            internalBuffer.release();
            internalBuffer = null;
        }
    }

    @Override
    public void close() {
        release();
    }

}
