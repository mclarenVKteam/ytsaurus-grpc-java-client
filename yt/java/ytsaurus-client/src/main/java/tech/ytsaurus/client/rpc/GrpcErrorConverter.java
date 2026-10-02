package tech.ytsaurus.client.rpc;

import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

import javax.annotation.Nullable;

import io.grpc.Metadata;
import io.grpc.Status;
import tech.ytsaurus.TError;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.core.common.YTsaurusErrorCode;

/**
 * Converts gRPC-level errors into exceptions to retry/failover logic.
 */
public final class GrpcErrorConverter {
    private static final String YT_ERROR_BIN_KEY = "yt-error-bin";

    private GrpcErrorConverter() {
    }

    /**
     * Converts a gRPC status into exception.
     *
     * @param status     gRPC status
     * @param trailers   response trailers (may contain {@code yt-error-bin})
     * @param isCanceled whether the local call was canceled by the caller
     * @return exception suitable for upper layers; never null
     */
    public static Throwable convert(Status status, @Nullable Metadata trailers, boolean isCanceled) {
        if (isCanceled) {
            return new CancellationException("gRPC call cancelled by client");
        }

        if (trailers != null) {
            byte[] ytErrorBytes = trailers.get(Metadata.Key.of(YT_ERROR_BIN_KEY, Metadata.BINARY_BYTE_MARSHALLER));
            if (ytErrorBytes != null && ytErrorBytes.length > 0) {
                try {
                    TError ytError = TError.parseFrom(ytErrorBytes);
                    return new YTsaurusError(ytError);
                } catch (Exception e) {
                    return new RuntimeException("Failed to parse yt-error-bin", e);
                }
            }
        }

        String errorMsg = status.getDescription() != null
                ? status.getDescription()
                : status.getCode().name();
        switch (status.getCode()) {
            case UNAVAILABLE:
                return new SocketException("gRPC unavailable: " + status.getDescription());
            case DEADLINE_EXCEEDED:
                return new SocketTimeoutException("gRPC deadline exceeded: " + status.getDescription());
            case CANCELLED:
                return new CancellationException("gRPC call cancelled: " + status.getDescription());
            case UNAUTHENTICATED:
            case PERMISSION_DENIED:
                // These are auth errors; wrap them as unrecoverable YTsaurus errors.
                return new YTsaurusError(TError.newBuilder()
                        .setCode(YTsaurusErrorCode.AuthenticationError.code)
                        .setMessage(errorMsg)
                        .build());
            default:
                return new RuntimeException("gRPC error: " + errorMsg, status.asRuntimeException());
        }
    }

    /**
     * Returns true if the error is a transient network error that justifies a retry.
     */
    public static boolean isRetryableNetworkError(Throwable error) {
        return error instanceof SocketException
                || error instanceof SocketTimeoutException
                || error instanceof TimeoutException;
    }
}
