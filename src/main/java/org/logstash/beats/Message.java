package org.logstash.beats;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectReader;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

public class Message implements Comparable<Message> {

    private final int sequence;
    private String identityStream;
    private Map<?, ?> data;
    private Batch batch;

    /**
     * Create a message using a map of key, value pairs
     * @param sequence sequence number of the message
     * @param map key/value pairs representing the message
     */
    public Message(int sequence, Map<?, ?> map) {
        this.sequence = sequence;
        this.data = Collections.unmodifiableMap(new HashMap<>(map));
    }

    /**
     * Create a message using a ByteBuf holding a Json object.
     * Note that this ctr is *lazy* - it will not deserialize the Json object until it is needed.
     * @param sequence sequence number of the message
     * @param buffer {@link ByteBuf} buffer containing Json object
     */
    public Message(int sequence, ByteBuf buffer) {
        this(sequence, buffer, DefaultJson.get());
    }

    @SuppressWarnings("unchecked")
    public Message(int sequence, ByteBuf buffer, ObjectReader reader) {
        this.sequence = sequence;
        if (buffer.isReadable()) {
            try (InputStream byteBufInputStream = new ByteBufInputStream(buffer)) {
                data = Collections.unmodifiableMap(reader.readValue(byteBufInputStream, Map.class));
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to parse beats payload ", e);
            }
        } else {
            data = Collections.emptyMap();
        }
    }

    /**
     * Returns the sequence number of this messsage
     * @return
     */
    public int getSequence() {
        return sequence;
    }

    /**
     * Returns a list of key/value pairs representing the contents of the message.
     * @return {@link Map} Map of key/value pairs
     */
    public Map<?, ?> getData() {
        return data;
    }

    @Override
    public int compareTo(Message o) {
        return Integer.compare(getSequence(), o.getSequence());
    }

    public Batch getBatch() {
        return batch;
    }

    public void setBatch(Batch batch) {
        this.batch = batch;
    }

    public String getIdentityStream() {
        if (identityStream == null) {
            identityStream = extractIdentityStream();
        }
        return identityStream;
    }

    private String extractIdentityStream() {
        @SuppressWarnings("unchecked")
        Map<String, String> beatsData = (Map<String, String>) getData().get("beat");

        if (beatsData != null) {
            String id = beatsData.get("id");
            String resourceId = beatsData.get("resource_id");

            if (id != null && resourceId != null) {
                return id + "-" + resourceId;
            } else {
                return beatsData.get("name") + "-" + beatsData.get("source");
            }
        } else {
            return null;
        }
    }

}
