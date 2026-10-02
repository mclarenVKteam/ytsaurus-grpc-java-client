package tech.ytsaurus.client.rpc;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.Nullable;

import io.grpc.CallOptions;
import io.grpc.ClientCall;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tech.ytsaurus.client.HostPort;
import tech.ytsaurus.core.GUID;
import tech.ytsaurus.rpc.TCredentialsExt;
import tech.ytsaurus.rpc.TRequestHeader;
import tech.ytsaurus.tracing.TTracingExt;

/**
 * gRPC-based implementation of {@link RpcClient}.
 *
 * <p>Maintains one {@link ManagedChannel} per destination address. The channel is created lazily,
 * cached while in use, and evicted after an idle timeout. When the owning pool bans the proxy,
 * the channel is shut down gracefully (with a short grace period for inflight requests).
 */
public class DefaultGrpcClient implements RpcClient {
    private static final Logger logger = LoggerFactory.getLogger(DefaultGrpcClient.class);

    private static final String API_SERVICE = "ApiService";
    private static final int DEFAULT_GRPC_PORT = 9014;

    private static final String YT_PROTOCOL_VERSION = "yt-protocol-version";
    private static final String YT_MESSAGE_BODY_SIZE = "yt-message-body-size";
    private static final String YT_REQUEST_ID = "yt-request-id";
    private static final String YT_AUTH_TOKEN = "yt-auth-token";
    private static final String YT_AUTH_USER_TICKET = "yt-auth-user-ticket";
    private static final String YT_AUTH_SERVICE_TICKET = "yt-auth-service-ticket";
    private static final String YT_TRACING_TRACE_ID = "yt-tracing-trace-id";
    private static final String YT_TRACING_SPAN_ID = "yt-tracing-span-id";
    private static final String YT_TRACING_SAMPLED = "yt-tracing-sampled";
    private static final String YT_TRACING_DEBUG = "yt-tracing-debug";

    private final String addressString;
    private final String destinationName;
    private final HostPort hostPort;
    private final YTsaurusClientAuth auth;
    private final RpcOptions options;
    private final ScheduledExecutorService executor;
    private final boolean ownedExecutor;
    private final boolean useTls;
    @Nullable
    private final String overrideAuthority;
    private final int maxInboundMessageSize;
    private final Duration idleTimeout;

    private final AtomicInteger referenceCounter = new AtomicInteger(1);
    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Nullable
    private volatile ManagedChannel channel;
    private volatile long lastUsedNanos = System.nanoTime();

    private final CompletableFuture<Void> closeFuture = new CompletableFuture<>();

    /**
     * Creates a gRPC RPC client.
     *
     * @param hostPort          proxy address
     * @param destinationName   logical destination name
     * @param auth              authentication data
     * @param options           RPC options
     * @param executor          optional scheduler for idle eviction and graceful shutdown;
     *                          if null, an internal daemon thread executor is created
     * @param useTls            whether to use TLS
     * @param overrideAuthority optional TLS authority override
     */
    public DefaultGrpcClient(
            HostPort hostPort,
            String destinationName,
            YTsaurusClientAuth auth,
            RpcOptions options,
            @Nullable ScheduledExecutorService executor,
            boolean useTls,
            @Nullable String overrideAuthority
    ) {
        this.hostPort = Objects.requireNonNull(hostPort);
        this.addressString = hostPort.toString();
        this.destinationName = Objects.requireNonNull(destinationName);
        this.auth = Objects.requireNonNull(auth);
        this.options = Objects.requireNonNull(options);
        this.useTls = useTls;
        this.overrideAuthority = overrideAuthority;
        this.maxInboundMessageSize = options.getGrpcMaxInboundMessageSize();
        this.idleTimeout = options.getGrpcIdleTimeout();
        if (executor != null) {
            this.executor = executor;
            this.ownedExecutor = false;
        } else {
            this.executor = java.util.concurrent.Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "yt-grpc-client-" + addressString);
                t.setDaemon(true);
                return t;
            });
            this.ownedExecutor = true;
        }
        scheduleIdleEviction();
    }

    private void scheduleIdleEviction() {
        if (idleTimeout.isZero() || idleTimeout.isNegative()) {
            return;
        }
        executor.schedule(this::checkIdleAndEvict, idleTimeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    private synchronized void checkIdleAndEvict() {
        if (closed.get()) {
            return;
        }
        long idleMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lastUsedNanos);
        if (idleMillis >= idleTimeout.toMillis()) {
            ManagedChannel ch = channel;
            channel = null;
            if (ch != null && !ch.isShutdown()) {
                logger.debug("Closing idle gRPC channel to {}", addressString);
                ch.shutdown();
            }
        }
        scheduleIdleEviction();
    }

    private ManagedChannel getOrCreateChannel() {
        ManagedChannel ch = channel;
        if (ch != null && !ch.isShutdown()) {
            lastUsedNanos = System.nanoTime();
            return ch;
        }

        // Build channel outside the synchronized block to avoid holding a global lock during DNS/TCP setup.
        ManagedChannel newChannel = buildChannel();

        synchronized (this) {
            if (closed.get()) {
                newChannel.shutdownNow();
                throw new IllegalStateException("Client is closed");
            }
            ch = channel;
            if (ch != null && !ch.isShutdown()) {
                newChannel.shutdownNow();
                return ch;
            }
            channel = newChannel;
            lastUsedNanos = System.nanoTime();
            return newChannel;
        }
    }

    private ManagedChannel buildChannel() {
        InetSocketAddress address = hostPort.toInetSocketAddress();
        NettyChannelBuilder builder = NettyChannelBuilder.forAddress(address);
        if (useTls) {
            builder.useTransportSecurity();
        } else {
            builder.usePlaintext();
        }
        if (overrideAuthority != null) {
            builder.overrideAuthority(overrideAuthority);
        }
        builder.maxInboundMessageSize(maxInboundMessageSize);
        // NB: NettyChannelBuilder does not expose maxOutboundMessageSize; enforce it manually if needed.
        // Keep the connection alive and detect broken channels quickly.
        builder.keepAliveTime(30, TimeUnit.SECONDS);
        builder.keepAliveTimeout(10, TimeUnit.SECONDS);
        builder.keepAliveWithoutCalls(true);
        return builder.build();
    }

    @Override
    public void ref() {
        int oldValue = referenceCounter.getAndIncrement();
        if (oldValue <= 0) {
            throw new IllegalStateException("Trying to ref dead object");
        }
    }

    @Override
    public void unref() {
        int newValue = referenceCounter.decrementAndGet();
        if (newValue < 0) {
            throw new IllegalStateException("Trying to unref dead object");
        }
        if (newValue == 0) {
            close();
        }
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        ManagedChannel ch;
        synchronized (this) {
            ch = channel;
            channel = null;
        }
        if (ch != null) {
            ch.shutdownNow();
        }
        if (ownedExecutor) {
            executor.shutdownNow();
        }
        closeFuture.complete(null);
    }

    @Override
    public RpcClientRequestControl send(
            RpcClient sender,
            RpcRequest<?> request,
            RpcClientResponseHandler handler,
            RpcOptions options
    ) {
        if (closed.get()) {
            handler.onError(new IllegalStateException("Client is closed"));
            return NoOpRequestControl.INSTANCE;
        }

        ManagedChannel ch = getOrCreateChannel();

        String methodName = request.header.getMethod();
        MethodDescriptor<byte[], byte[]> method = MethodDescriptor.<byte[], byte[]>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(API_SERVICE + "/" + methodName)
                .setRequestMarshaller(YtRawMarshaller.INSTANCE)
                .setResponseMarshaller(YtRawMarshaller.INSTANCE)
                .build();

        byte[] requestBody = serializeBody(request);
        List<byte[]> attachments = request.compressedAttachments != null
                ? request.compressedAttachments
                : request.attachments;
        byte[] grpcPayload = GrpcMessageUtils.packMessage(requestBody, attachments);

        Metadata metadata = buildMetadata(request);

        CallOptions callOptions = CallOptions.DEFAULT
                .withDeadlineAfter(getCallDeadlineMillis(request), TimeUnit.MILLISECONDS);

        ClientCall<byte[], byte[]> call = ch.newCall(method, callOptions);
        GrpcCallListener listener = new GrpcCallListener(call, handler, sender, request.header);
        call.start(listener, metadata);
        call.request(1);
        call.sendMessage(grpcPayload);
        call.halfClose();

        return listener;
    }

    private long getCallDeadlineMillis(RpcRequest<?> request) {
        Duration requestTimeout = RpcRequest.getTimeout(request.header);
        if (requestTimeout != null && !requestTimeout.isZero() && !requestTimeout.isNegative()) {
            return requestTimeout.toMillis();
        }
        return options.getGlobalTimeout().toMillis();
    }

    private byte[] serializeBody(RpcRequest<?> request) {
        int size = request.body.getSerializedSize();
        byte[] body = new byte[size];
        try {
            request.body.writeTo(com.google.protobuf.CodedOutputStream.newInstance(body));
        } catch (IOException e) {
            throw new java.io.UncheckedIOException("Failed to serialize request body", e);
        }
        return body;
    }

    private Metadata buildMetadata(RpcRequest<?> request) {
        Metadata metadata = new Metadata();
        putAscii(metadata, YT_PROTOCOL_VERSION, options.getGrpcProtocolVersion());
        putAscii(metadata, YT_MESSAGE_BODY_SIZE, String.valueOf(request.body.getSerializedSize()));
        putAscii(metadata, YT_REQUEST_ID, RpcUtil.fromProto(request.header.getRequestId()).toString());

        // Authentication.
        if (request.header.hasExtension(TCredentialsExt.credentialsExt)) {
            TCredentialsExt credentialsExt = request.header.getExtension(TCredentialsExt.credentialsExt);
            if (credentialsExt.hasToken()) {
                putAscii(metadata, YT_AUTH_TOKEN, credentialsExt.getToken());
            } else if (credentialsExt.hasUserTicket()) {
                putAscii(metadata, YT_AUTH_USER_TICKET, credentialsExt.getUserTicket());
            } else if (credentialsExt.hasServiceTicket()) {
                putAscii(metadata, YT_AUTH_SERVICE_TICKET, credentialsExt.getServiceTicket());
            }
        } else {
            if (auth.getToken().isPresent()) {
                putAscii(metadata, YT_AUTH_TOKEN, auth.getToken().get());
            } else if (auth.getUserTicketAuth().isPresent()) {
                putAscii(metadata, YT_AUTH_USER_TICKET, auth.getUserTicketAuth().get().issueUserTicket());
            } else if (auth.getServiceTicketAuth().isPresent()) {
                putAscii(metadata, YT_AUTH_SERVICE_TICKET, auth.getServiceTicketAuth().get().issueServiceTicket());
            }
        }

        // Tracing.
        if (request.header.hasExtension(TRequestHeader.tracingExt)) {
            TTracingExt tracing = request.header.getExtension(TRequestHeader.tracingExt);
            if (tracing.hasTraceId()) {
                GUID traceId = RpcUtil.fromProto(tracing.getTraceId());
                putAscii(metadata, YT_TRACING_TRACE_ID, traceId.toString());
            }
            if (tracing.hasSpanId()) {
                putAscii(metadata, YT_TRACING_SPAN_ID, String.valueOf(tracing.getSpanId()));
            }
            if (tracing.getSampled()) {
                putAscii(metadata, YT_TRACING_SAMPLED, "1");
            }
            if (tracing.getDebug()) {
                putAscii(metadata, YT_TRACING_DEBUG, "1");
            }
        }

        return metadata;
    }

    private static void putAscii(Metadata metadata, String key, String value) {
        metadata.put(Metadata.Key.of(key, Metadata.ASCII_STRING_MARSHALLER), value);
    }

    @Override
    public RpcClientStreamControl startStream(
            RpcClient sender,
            RpcRequest<?> request,
            RpcStreamConsumer consumer,
            RpcOptions options
    ) {
        // Streaming over gRPC is not supported in the initial implementation.
        throw new UnsupportedOperationException(
                "gRPC streaming is not supported yet; use bus transport for table/file operations");
    }

    @Override
    public String destinationName() {
        return destinationName;
    }

    @Override
    public String getAddressString() {
        return addressString;
    }

    @Override
    public ScheduledExecutorService executor() {
        return executor;
    }

    @Override
    public String toString() {
        return String.format("GrpcRpcClient(%s@%s)", destinationName, addressString);
    }

    /**
     * Listener that bridges a gRPC unary call to {@link RpcClientResponseHandler}.
     */
    private static class GrpcCallListener extends ClientCall.Listener<byte[]>
            implements RpcClientRequestControl {
        private final ClientCall<byte[], byte[]> call;
        private final RpcClientResponseHandler handler;
        private final RpcClient sender;
        private final TRequestHeader requestHeader;
        private final AtomicBoolean canceled = new AtomicBoolean(false);
        private final AtomicBoolean finished = new AtomicBoolean(false);

        @Nullable
        private Metadata trailers;
        @Nullable
        private byte[] message;

        GrpcCallListener(
                ClientCall<byte[], byte[]> call,
                RpcClientResponseHandler handler,
                RpcClient sender,
                TRequestHeader requestHeader
        ) {
            this.call = call;
            this.handler = handler;
            this.sender = sender;
            this.requestHeader = requestHeader;
        }

        @Override
        public void onHeaders(Metadata headers) {
            // YT does not use headers for response metadata; everything is in trailers.
        }

        @Override
        public void onMessage(byte[] message) {
            this.message = message;
        }

        private byte[] wrapInYtEnvelope(byte[] body) {
            byte[] envelope = new byte[8 + body.length];
            java.nio.ByteBuffer buffer = java.nio.ByteBuffer.wrap(envelope).order(java.nio.ByteOrder.LITTLE_ENDIAN);
            buffer.putInt(0); // envelope header size
            buffer.putInt(body.length);
            System.arraycopy(body, 0, envelope, 8, body.length);
            return envelope;
        }

        @Override
        public void onClose(Status status, Metadata trailers) {
            this.trailers = trailers;
            if (!status.isOk()) {
                finishWithError(GrpcErrorConverter.convert(status, trailers, canceled.get()));
                return;
            }
            if (message == null) {
                finishWithError(new IllegalStateException("No gRPC response message"));
                return;
            }
            try {
                int bodySize = readBodySize(trailers, message.length);
                GrpcMessageUtils.MessageWithAttachments parts = GrpcMessageUtils.splitMessage(message, bodySize);

                // Java client expects response body as the first attachment, followed by rowset attachments.
                // The body must be wrapped in the YT serialized-message envelope that LazyResponse understands.
                List<byte[]> responseAttachments = new ArrayList<>(1 + parts.getAttachments().size());
                responseAttachments.add(wrapInYtEnvelope(parts.getBody()));
                responseAttachments.addAll(parts.getAttachments());

                tech.ytsaurus.rpc.TResponseHeader.Builder headerBuilder =
                        tech.ytsaurus.rpc.TResponseHeader.newBuilder();
                // The gRPC response body is the raw protobuf response; there is no separate YT response header.
                // We build a minimal TResponseHeader for compatibility with the rest of the stack.
                headerBuilder.setRequestId(requestHeader.getRequestId());
                handler.onResponse(sender, headerBuilder.build(), responseAttachments);
            } catch (Throwable e) {
                finishWithError(e);
            }
        }

        @Override
        public boolean cancel() {
            if (!canceled.compareAndSet(false, true)) {
                return false;
            }
            try {
                call.cancel("Cancelled by user", new CancellationException());
            } finally {
                finishWithError(new CancellationException("Request cancelled"));
            }
            return true;
        }

        private void finishWithError(Throwable error) {
            if (finished.compareAndSet(false, true)) {
                if (error instanceof CancellationException) {
                    handler.onCancel((CancellationException) error);
                } else {
                    handler.onError(error);
                }
            }
        }

        /**
         * Reads the protobuf body size from trailing metadata.
         * The server only sends {@code yt-message-body-size} when the response has attachments;
         * otherwise the whole gRPC payload is the body.
         */
        private static int readBodySize(@Nullable Metadata trailers, int payloadSize) {
            if (trailers == null) {
                return payloadSize;
            }
            String value = trailers.get(Metadata.Key.of(YT_MESSAGE_BODY_SIZE, Metadata.ASCII_STRING_MARSHALLER));
            if (value == null) {
                return payloadSize;
            }
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new IllegalStateException("Invalid yt-message-body-size: " + value, e);
            }
        }
    }

    private static class NoOpRequestControl implements RpcClientRequestControl {
        static final NoOpRequestControl INSTANCE = new NoOpRequestControl();

        @Override
        public boolean cancel() {
            return false;
        }
    }
}
