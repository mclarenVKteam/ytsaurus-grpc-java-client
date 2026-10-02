package tech.ytsaurus.client.rpc;

import java.io.InputStream;
import java.io.ByteArrayInputStream;

import io.grpc.MethodDescriptor;

/**
 * gRPC marshaller that passes raw bytes without re-serializing protobuf.
 *
 * <p>The YT request/response body is already serialized protobuf; this marshaller
 * transmits the byte array as-is inside a single unary gRPC message.
 */
public final class YtRawMarshaller implements MethodDescriptor.Marshaller<byte[]> {
    public static final YtRawMarshaller INSTANCE = new YtRawMarshaller();

    private YtRawMarshaller() {
    }

    @Override
    public byte[] parse(InputStream stream) {
        try {
            return stream.readAllBytes();
        } catch (Exception e) {
            throw new RuntimeException("Failed to read raw gRPC response", e);
        }
    }

    @Override
    public InputStream stream(byte[] value) {
        return new ByteArrayInputStream(value);
    }
}
