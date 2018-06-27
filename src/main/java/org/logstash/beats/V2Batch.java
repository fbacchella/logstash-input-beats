package org.logstash.beats;

import java.util.Iterator;
import java.util.NoSuchElementException;

import org.logstash.beats.BeatsParser.InvalidFrameProtocolException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

/**
 * Implementation of {@link Batch} for the v2 protocol backed by ByteBuf. *must* be released after use.
 */
public class V2Batch implements Batch {

    private ByteBuf internalBuffer = PooledByteBufAllocator.DEFAULT.buffer();
    private int written = 0;
    private static final int SIZE_OF_INT = 4;
    private int batchSize;
    private final int maxPayloadSize;

    public V2Batch() {
        maxPayloadSize = -1;
    }

    public V2Batch(int maxPayloadSize) {
        this.maxPayloadSize = maxPayloadSize;
    }

    @Override
    public byte getProtocol() {
        return Protocol.VERSION_2;
    }

    public Iterator<Message> iterator() {
        return new Iterator<Message>() {
            private int read = 0;
            private ByteBuf readerBuffer = internalBuffer.asReadOnly().resetReaderIndex();
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
                Message message = new Message(sequenceNumber, readerBuffer.slice(readerBuffer.readerIndex(), readableBytes));
                readerBuffer.readerIndex(readerBuffer.readerIndex() + readableBytes);
                message.setBatch(V2Batch.this);
                read++;
                return message;
            }
        };
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

    /**
     * Adds a message to the batch, which will be constructed into an actual {@link Message} lazily.
     * @param sequenceNumber sequence number of the message within the batch
     * @param buffer A ByteBuf pointing to serialized JSon
     * @param size size of the serialized Json
     * @throws InvalidFrameProtocolException 
     */
    void addMessage(int sequenceNumber, ByteBuf buffer, int size) throws InvalidFrameProtocolException {
        written++;
        if (internalBuffer.writableBytes() < size + (2 * SIZE_OF_INT)) {
            // increase size slightly faster than what is really need,
            // to avoid doing an alloc for each message
            int newSize = (int) ((internalBuffer.capacity() + size + (2 * SIZE_OF_INT)) * 1.5);
            if (maxPayloadSize > 0 && newSize > maxPayloadSize) {
                release();
                throw new InvalidFrameProtocolException("Oversized payload: " + newSize);
            }
            internalBuffer.capacity(newSize);
        }
        internalBuffer.writeInt(sequenceNumber);
        internalBuffer.writeInt(size);
        buffer.readBytes(internalBuffer, size);
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
