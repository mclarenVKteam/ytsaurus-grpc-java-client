package tech.ytsaurus.client.rpc;

import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.CancellationException;

import io.grpc.Metadata;
import io.grpc.Status;
import org.junit.Assert;
import org.junit.Test;
import tech.ytsaurus.TError;
import tech.ytsaurus.core.common.YTsaurusError;
import tech.ytsaurus.core.common.YTsaurusErrorCode;

public class GrpcErrorConverterTest {
    @Test
    public void testUnavailableBecomesSocketException() {
        Throwable error = GrpcErrorConverter.convert(
                Status.UNAVAILABLE.withDescription("connection refused"), null, false);
        Assert.assertTrue(error instanceof SocketException);
        Assert.assertTrue(GrpcErrorConverter.isRetryableNetworkError(error));
    }

    @Test
    public void testDeadlineExceededBecomesSocketTimeoutException() {
        Throwable error = GrpcErrorConverter.convert(
                Status.DEADLINE_EXCEEDED.withDescription("timeout"), null, false);
        Assert.assertTrue(error instanceof SocketTimeoutException);
        Assert.assertTrue(GrpcErrorConverter.isRetryableNetworkError(error));
    }

    @Test
    public void testYtErrorBinHasPriority() {
        TError ytError = TError.newBuilder()
                .setCode(YTsaurusErrorCode.ProxyBanned.code)
                .setMessage("Proxy is banned")
                .build();
        Metadata trailers = new Metadata();
        trailers.put(
                Metadata.Key.of("yt-error-bin", Metadata.BINARY_BYTE_MARSHALLER),
                ytError.toByteArray());

        Throwable error = GrpcErrorConverter.convert(Status.OK, trailers, false);
        Assert.assertTrue(error instanceof YTsaurusError);
    }

    @Test
    public void testLocalCancellationWins() {
        Throwable error = GrpcErrorConverter.convert(
                Status.CANCELLED.withDescription("cancelled"), null, true);
        Assert.assertTrue(error instanceof CancellationException);
        Assert.assertFalse(GrpcErrorConverter.isRetryableNetworkError(error));
    }
}
