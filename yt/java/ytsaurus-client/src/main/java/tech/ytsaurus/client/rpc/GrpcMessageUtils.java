package tech.ytsaurus.client.rpc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

/**
 * Utilities for packing/unpacking YT request/response messages over gRPC.
 *
 * <p>The gRPC message consists of the serialized protobuf body followed by attachment payloads.
 * Each attachment is prefixed with a 4-byte little-endian length. The special value
 * {@code 0xFFFFFFFF} denotes an omitted (null) attachment.
 */
public final class GrpcMessageUtils {
    private GrpcMessageUtils() {
    }

    /**
     * Special length marker for omitted attachments.
     */
    public static final int OMITTED_ATTACHMENT = 0xFFFFFFFF;

    /**
     * Packs protobuf body and attachments into a single byte array.
     */
    public static byte[] packMessage(byte[] body, @Nullable List<byte[]> attachments) {
        int totalSize = body.length;
        int attachmentCount = attachments != null ? attachments.size() : 0;
        for (int i = 0; i < attachmentCount; i++) {
            byte[] attachment = attachments.get(i);
            totalSize += 4;
            if (attachment != null) {
                totalSize += attachment.length;
            }
        }

        byte[] result = new byte[totalSize];
        ByteBuffer buffer = ByteBuffer.wrap(result).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put(body);
        for (int i = 0; i < attachmentCount; i++) {
            byte[] attachment = attachments.get(i);
            if (attachment == null) {
                buffer.putInt(OMITTED_ATTACHMENT);
            } else {
                buffer.putInt(attachment.length);
                buffer.put(attachment);
            }
        }
        return result;
    }

    /**
     * Splits a raw gRPC response into body and attachments using the body size from trailers.
     */
    public static MessageWithAttachments splitMessage(byte[] rawResponse, int bodySize) {
        if (bodySize < 0 || bodySize > rawResponse.length) {
            throw new IllegalArgumentException(String.format(
                    "yt-message-body-size %d is outside payload %d", bodySize, rawResponse.length));
        }

        byte[] body = new byte[bodySize];
        System.arraycopy(rawResponse, 0, body, 0, bodySize);

        List<byte[]> attachments = parseAttachments(rawResponse, bodySize, rawResponse.length - bodySize);
        return new MessageWithAttachments(body, attachments);
    }

    private static List<byte[]> parseAttachments(byte[] buffer, int offset, int length) {
        List<byte[]> result = new ArrayList<>();
        ByteBuffer bb = ByteBuffer.wrap(buffer, offset, length).order(ByteOrder.LITTLE_ENDIAN);
        while (bb.hasRemaining()) {
            if (bb.remaining() < 4) {
                throw new IllegalArgumentException("Truncated attachment header");
            }
            int len = bb.getInt();
            if (len == OMITTED_ATTACHMENT) {
                result.add(null);
                continue;
            }
            if (len < 0 || len > bb.remaining()) {
                throw new IllegalArgumentException(String.format(
                        "Truncated attachment: want %d have %d", len, bb.remaining()));
            }
            byte[] attachment = new byte[len];
            bb.get(attachment);
            result.add(attachment);
        }
        return result;
    }

    /**
     * Result of splitting a raw gRPC payload into a protobuf body and a list of attachments.
     */
    public static class MessageWithAttachments {
        private final byte[] body;
        private final List<byte[]> attachments;

        public MessageWithAttachments(byte[] body, List<byte[]> attachments) {
            this.body = body;
            this.attachments = attachments;
        }

        /** Returns the serialized protobuf body. */
        public byte[] getBody() {
            return body;
        }

        /** Returns the attachments extracted from the payload. */
        public List<byte[]> getAttachments() {
            return attachments;
        }
    }
}
