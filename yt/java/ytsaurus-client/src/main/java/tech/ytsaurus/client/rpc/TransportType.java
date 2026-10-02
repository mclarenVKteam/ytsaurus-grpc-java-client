package tech.ytsaurus.client.rpc;

/**
 * Transport type used by the client to communicate with YTsaurus proxies.
 */
public enum TransportType {
    /**
     * Native YT bus transport; the default.
     */
    BUS,
    /**
     * gRPC transport over HTTP/2.
     */
    GRPC
}
