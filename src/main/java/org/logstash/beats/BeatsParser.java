package org.logstash.beats;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.Inflater;
import java.util.zip.InflaterOutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

public class BeatsParser extends ByteToMessageDecoder {

    private static final Logger logger = LogManager.getLogger();

    private Batch batch;

    private enum States {
        READ_HEADER(1),
        READ_FRAME_TYPE(1),
        READ_WINDOW_SIZE(4),
        READ_JSON_HEADER(8),
        READ_COMPRESSED_FRAME_HEADER(4),
        READ_COMPRESSED_FRAME(-1), // -1 means the length to read is variable and defined in the frame itself.
        READ_JSON(-1),
        READ_DATA_FIELDS(-1);

        private int length;

        States(int length) {
            this.length = length;
        }

    }

    private States currentState = States.READ_HEADER;
    private int requiredBytes = States.READ_HEADER.length;
    private int sequence = 0;
    private final int maxPayloadSize;
    private boolean decodingCompressedBuffer = false;

    /**
     * Create a beats parser with no maximum payload size check.
     */
    public BeatsParser() {
        maxPayloadSize = Integer.MAX_VALUE;
    }

    /**
     * Create a parser with a maximum payload size. Any value less or equal to 0 disable it.
     * 
     * @param maxPayloadSize
     */
    public BeatsParser(int maxPayloadSize) {
        this.maxPayloadSize = maxPayloadSize >= 0 ? maxPayloadSize : Integer.MAX_VALUE;
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws InvalidFrameProtocolException, IOException {
        try {
            if (!hasEnoughBytes(in)) {
                if (decodingCompressedBuffer){
                    throw new InvalidFrameProtocolException("Insufficient bytes in compressed content to decode: " + currentState);
                } else {
                    return;
                }
            }

            switch (currentState) {
            case READ_HEADER: {
                logger.trace("Running: READ_HEADER");

                byte currentVersion = in.readByte();
                if (batch == null) {
                    if (Protocol.isVersion2(currentVersion)) {
                        logger.trace("Frame version 2 detected");
                        batch = new V2Batch(maxPayloadSize);
                    } else if (Protocol.isVersion1(currentVersion)) {
                        logger.trace("Frame version 1 detected");
                        batch = new V1Batch();
                    } else {
                        throw new InvalidFrameProtocolException("Unsupported protocol version: " + currentVersion);
                    }
                }
                transition(States.READ_FRAME_TYPE);
                break;
            }
            case READ_FRAME_TYPE: {
                byte frameType = in.readByte();

                switch (frameType) {
                case Protocol.CODE_WINDOW_SIZE:
                    transition(States.READ_WINDOW_SIZE);
                    break;
                case Protocol.CODE_JSON_FRAME:
                    // Reading Sequence + size of the payload
                    transition(States.READ_JSON_HEADER);
                    break;
                case Protocol.CODE_COMPRESSED_FRAME:
                    transition(States.READ_COMPRESSED_FRAME_HEADER);
                    break;
                case Protocol.CODE_FRAME:
                    transition(States.READ_DATA_FIELDS);
                    break;
                default:
                    throw new InvalidFrameProtocolException("Invalid Frame Type, received: " + frameType);
                }
                break;
            }
            case READ_WINDOW_SIZE: {
                logger.trace("Running: READ_WINDOW_SIZE");
                int batchSize = tryReadUnsigned(in, "Invalid window size", true);
                batch.setBatchSize(batchSize);
                logger.debug("New window size is {}", batchSize);

                // This is unlikely to happen but I have no way to known when a frame is
                // actually completely done other than checking the windows and the sequence number,
                // If the FSM read a new window and I have still
                // events buffered I should send the current batch down to the next handler.
                if (!batch.isEmpty()) {
                    logger.warn("New window size received but the current batch was not complete, sending the current batch");
                    batchComplete(out);
                }

                transition(States.READ_HEADER);
                break;
            }
            case READ_DATA_FIELDS: {
                // Lumberjack version 1 protocol, which use the Key:Value format.
                logger.trace("Running: READ_DATA_FIELDS");
                sequence = tryReadUnsigned(in, "Invalid sequence number", true);
                int fieldsCount = tryReadUnsigned(in, "Invalid number of fields", false);
                
                // Using a rough estimation of the size of an empty HashMap
                if (fieldsCount * 4 > maxPayloadSize) {
                    throw new InvalidFrameProtocolException("Oversized entry count: " + fieldsCount);
                }

                Map<String, String> dataMap = new HashMap<>(fieldsCount);

                // Use a long to avoid overflow
                long currentPayload = 0;
                for (int count = 0; count < fieldsCount ; count++) {
                    currentPayload += 32;  // 32 is the size of the HashMap.Entry
                    int fieldLength= tryReadUnsigned(in, "Oversized field name length", false);
                    currentPayload += fieldLength;
                    if (currentPayload > maxPayloadSize) {
                        throw new InvalidFrameProtocolException("Oversized payload: " + currentPayload);
                    }
                    ByteBuf fieldBuf = in.readBytes(fieldLength);
                    String field = fieldBuf.toString(StandardCharsets.UTF_8);
                    fieldBuf.release();

                    int dataLength = tryReadUnsigned(in, "Oversized field data length", true);
                    currentPayload += dataLength;
                    if (currentPayload > maxPayloadSize) {
                        throw new InvalidFrameProtocolException("Oversized payload: " + currentPayload);
                    }
                    ByteBuf dataBuf = in.readBytes(dataLength);
                    String data = dataBuf.toString(StandardCharsets.UTF_8);
                    dataBuf.release();

                    dataMap.put(field, data);
                }
                Message message = new Message(sequence, dataMap);
                ((V1Batch) batch).addMessage(message);

                if (batch.isComplete()) {
                    batchComplete(out);
                }
                transition(States.READ_HEADER);

                break;
            }
            case READ_JSON_HEADER: {
                logger.trace("Running: READ_JSON_HEADER");
                sequence = tryReadUnsigned(in, "Invalid sequence number", true);
                int jsonPayloadSize = tryReadUnsigned(in, "Invalid json length", false);
                transition(States.READ_JSON, jsonPayloadSize);
                break;
            }
            case READ_COMPRESSED_FRAME_HEADER: {
                logger.trace("Running: READ_COMPRESSED_FRAME_HEADER");
                int compressedFrameSize = tryReadUnsigned(in, "Invalid compressed frame size", false);
                transition(States.READ_COMPRESSED_FRAME, compressedFrameSize);
                break;
            }
            case READ_COMPRESSED_FRAME: {
                logger.trace("Running: READ_COMPRESSED_FRAME");
                // Use the compressed size as the safe start for the buffer.
                ByteBuf buffer;
                buffer = inflateCompressedFrame(ctx, in);
                transition(States.READ_HEADER);

                decodingCompressedBuffer = true;
                try {
                    while (buffer.readableBytes() > 0) {
                        decode(ctx, buffer, out);
                    }
                } finally {
                    decodingCompressedBuffer = false;
                    buffer.release();
                    transition(States.READ_HEADER);
                }
                break;
            }
            case READ_JSON: {
                logger.trace("Running: READ_JSON");
                try {
                    ((V2Batch) batch).addMessage(sequence, in, requiredBytes);
                } catch (InvalidFrameProtocolException | RuntimeException ex) {
                    throw ex;
                }
                if (batch.isComplete()) {
                    logger.trace("{}", () -> "Sending batch size: " + batch.size() + ", windowSize: " + batch.getBatchSize() + " , seq: " + sequence);
                    batchComplete(out);
                }
                transition(States.READ_HEADER);
                break;
            }
            }
        } catch (InvalidFrameProtocolException | RuntimeException | IOException e) {
            resetOnError(in, out);
            throw e;
        }
    }

    private ByteBuf inflateCompressedFrame(ChannelHandlerContext ctx, ByteBuf in) throws IOException, InvalidFrameProtocolException {
        ByteBuf buffer = ctx.alloc().buffer(requiredBytes);
        Inflater inflater = new Inflater();
        try (ByteBufOutputStream buffOutput = new ByteBufOutputStream(buffer);
             InflaterOutputStream inflaterStream = new InflaterOutputStream(buffOutput, inflater)
        ) {
            in.readBytes(inflaterStream, requiredBytes);
            if (buffer.readableBytes() > maxPayloadSize) {
                throw new InvalidFrameProtocolException("Oversized compressed payload: " + buffer.readableBytes());
            }
        } catch (IOException | InvalidFrameProtocolException | RuntimeException ex) {
            buffer.release();
            throw ex;
        } finally {
            inflater.end();
        }
        return buffer;
    }

    private boolean hasEnoughBytes(ByteBuf in) {
        return in.readableBytes() >= requiredBytes;
    }

    private void transition(States next) throws InvalidFrameProtocolException {
        transition(next, next.length);
    }

    private void transition(States next, int requiredBytes) throws InvalidFrameProtocolException {
        logger.trace("{}", () -> "Transition, from: " + currentState + ", to: " + next + ", requiring " + requiredBytes + " bytes");
        this.currentState = next;
        this.requiredBytes = requiredBytes;
    }

    private void batchComplete(List<Object> out) {
        requiredBytes = 0;
        sequence = 0;
        out.add(batch);
        batch = null;
    }
    
    private void resetOnError(ByteBuf in, List<Object> out) {
        in.clear();
        currentState = States.READ_HEADER;
        requiredBytes = States.READ_HEADER.length;
        if (batch != null) {
            out.add(batch);
            batch = null;
        }
    }

    private int tryReadUnsigned(ByteBuf in, String message, boolean canZero) throws InvalidFrameProtocolException {
        long trylong = in.readUnsignedInt();
        if (trylong == 0 && ! canZero) {
            throw new InvalidFrameProtocolException(message + ", received: " + trylong);
        }
        try {
            return Math.toIntExact(trylong);
        } catch (ArithmeticException e) {
            throw new InvalidFrameProtocolException(message + ", received: " + trylong);
        }
    }

    public static class InvalidFrameProtocolException extends Exception {
        InvalidFrameProtocolException(String message) {
            super(message);
        }
    }

}
