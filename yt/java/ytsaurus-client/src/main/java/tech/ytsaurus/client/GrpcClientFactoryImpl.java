package tech.ytsaurus.client;

import javax.annotation.Nullable;

import tech.ytsaurus.client.rpc.DefaultGrpcClient;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;
import tech.ytsaurus.lang.NonNullApi;
import tech.ytsaurus.lang.NonNullFields;

/**
 * Factory for creating gRPC-based {@link RpcClient} instances.
 */
@NonNullApi
@NonNullFields
public class GrpcClientFactoryImpl implements RpcClientFactory {
    private final YTsaurusClientAuth auth;
    private final RpcOptions options;
    private final boolean useTls;
    @Nullable
    private final String overrideAuthority;

    /**
     * Creates a factory.
     *
     * @param auth              client authentication data
     * @param options           RPC options, including gRPC-specific settings
     * @param useTls            whether to use TLS for gRPC connections
     * @param overrideAuthority optional value for {@link io.grpc.ManagedChannelBuilder#overrideAuthority}
     */
    public GrpcClientFactoryImpl(
            YTsaurusClientAuth auth,
            RpcOptions options,
            boolean useTls,
            @Nullable String overrideAuthority
    ) {
        this.auth = auth;
        this.options = options;
        this.useTls = useTls;
        this.overrideAuthority = overrideAuthority;
    }

    /**
     * Creates a {@link DefaultGrpcClient} for the given address.
     *
     * <p>If {@code hostPort} does not contain an explicit port, the default gRPC proxy port 9014 is used.
     */
    @Override
    public RpcClient create(HostPort hostPort, String name) {
        HostPort grpcHostPort = HostPort.parse(ensureGrpcPort(hostPort.toString()));
        return new DefaultGrpcClient(
                grpcHostPort,
                name,
                auth,
                options,
                null,
                useTls,
                overrideAuthority
        );
    }

    private static String ensureGrpcPort(String address) {
        if (address.startsWith("[")) {
            int closeBracket = address.lastIndexOf(']');
            if (closeBracket == address.length() - 1 || address.charAt(closeBracket + 1) != ':') {
                return address + ":9014";
            }
        } else if (!address.contains(":")) {
            return address + ":9014";
        }
        return address;
    }
}
