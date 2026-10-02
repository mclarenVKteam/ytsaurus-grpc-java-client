package tech.ytsaurus.client;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import tech.ytsaurus.client.bus.DefaultBusConnector;
import tech.ytsaurus.client.rpc.RpcClient;
import tech.ytsaurus.client.rpc.RpcClientRequestControl;
import tech.ytsaurus.client.rpc.RpcClientResponseHandler;
import tech.ytsaurus.client.rpc.RpcClientStreamControl;
import tech.ytsaurus.client.rpc.RpcOptions;
import tech.ytsaurus.client.rpc.RpcRequest;
import tech.ytsaurus.client.rpc.RpcStreamConsumer;
import tech.ytsaurus.client.rpc.YTsaurusClientAuth;

public class DisableProxyDiscoveryTest {
    private DefaultBusConnector connector;

    @Before
    public void setUp() {
        connector = new DefaultBusConnector();
    }

    @After
    public void tearDown() {
        connector.close();
    }

    @Test
    public void noAddressesOrBalancerWithDisabledDiscoveryIsRejected() {
        Assert.assertThrows(IllegalArgumentException.class, () -> new YTsaurusClient.ClientPoolProvider(
                connector,
                List.of(new YTsaurusCluster("adtech-profile", null, 0, List.of())),
                null,
                null,
                null,
                false,
                false,
                false,
                true,
                YTsaurusClientAuth.empty(),
                new SimpleMockFactory(),
                new RpcOptions(),
                Runnable::run));
    }

    @Test
    public void balancerFqdnIsUsedAsFallbackWhenDiscoveryDisabled() throws Exception {
        SimpleMockFactory factory = new SimpleMockFactory();
        RpcOptions options = new RpcOptions();

        try (YTsaurusClient.ClientPoolProvider provider = new YTsaurusClient.ClientPoolProvider(
                connector,
                List.of(new YTsaurusCluster("pc.yt-rpc-proxy-adtech-profile.mesh.local:6000")),
                null,
                null,
                null,
                false,
                false,
                false,
                true,
                YTsaurusClientAuth.empty(),
                factory,
                options,
                Runnable::run)) {

            CompletableFuture<Void> release = new CompletableFuture<>();
            try {
                CompletableFuture<RpcClient> clientFuture = provider.getClientPool().peekClient(release);
                RpcClient client = clientFuture.get(5, TimeUnit.SECONDS);
                Assert.assertEquals(
                        "pc.yt-rpc-proxy-adtech-profile.mesh.local:6000",
                        client.getAddressString());
                Assert.assertTrue(factory.isConnectionOpened("pc.yt-rpc-proxy-adtech-profile.mesh.local:6000"));
            } finally {
                release.complete(null);
            }
        }
    }

    @Test
    public void staticMeshProxiesAreUsedWhenDiscoveryDisabled() throws Exception {
        SimpleMockFactory factory = new SimpleMockFactory();
        RpcOptions options = new RpcOptions();

        try (YTsaurusClient.ClientPoolProvider provider = new YTsaurusClient.ClientPoolProvider(
                connector,
                List.of(new YTsaurusCluster(
                        "adtech-profile",
                        null,
                        0,
                        List.of(
                                "pc.yt-rpc-proxy-adtech-profile.mesh.local:6000",
                                "rc.yt-rpc-proxy-adtech-profile.mesh.local:6000"
                        ))),
                null,
                null,
                null,
                false,
                false,
                false,
                true,
                YTsaurusClientAuth.empty(),
                factory,
                options,
                Runnable::run)) {

            CompletableFuture<Void> release = new CompletableFuture<>();
            try {
                CompletableFuture<RpcClient> clientFuture = provider.getClientPool().peekClient(release);
                RpcClient client = clientFuture.get(5, TimeUnit.SECONDS);
                String address = client.getAddressString();
                Assert.assertTrue(
                        "Expected mesh proxy address, got: " + address,
                        address.equals("pc.yt-rpc-proxy-adtech-profile.mesh.local:6000")
                                || address.equals("rc.yt-rpc-proxy-adtech-profile.mesh.local:6000"));
                Assert.assertTrue(factory.isConnectionOpened(address));
            } finally {
                release.complete(null);
            }
        }
    }

    private static class SimpleMockFactory implements RpcClientFactory {
        private final java.util.Set<HostPort> openedConnections = new java.util.HashSet<>();
        private final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

        boolean isConnectionOpened(String hostPort) {
            return openedConnections.contains(HostPort.parse(hostPort));
        }

        @Override
        public RpcClient create(HostPort hostPort, String name) {
            if (openedConnections.contains(hostPort)) {
                throw new RuntimeException("Connection to " + hostPort + " is already opened");
            }
            openedConnections.add(hostPort);
            return new SimpleMockClient(hostPort, executor);
        }
    }

    private static class SimpleMockClient implements RpcClient {
        private final HostPort hostPort;
        private final ScheduledExecutorService executor;
        private final java.util.concurrent.atomic.AtomicInteger refCounter =
                new java.util.concurrent.atomic.AtomicInteger(1);

        SimpleMockClient(HostPort hostPort, ScheduledExecutorService executor) {
            this.hostPort = hostPort;
            this.executor = executor;
        }

        @Override
        public void ref() {
            int oldValue = refCounter.getAndIncrement();
            if (oldValue <= 0) {
                throw new IllegalStateException();
            }
        }

        @Override
        public void unref() {
            int newValue = refCounter.decrementAndGet();
            if (newValue < 0) {
                throw new IllegalStateException();
            }
        }

        @Override
        public void close() {
        }

        @Override
        public RpcClientRequestControl send(
                RpcClient sender, RpcRequest<?> request, RpcClientResponseHandler handler, RpcOptions options) {
            return null;
        }

        @Override
        public RpcClientStreamControl startStream(
                RpcClient sender, RpcRequest<?> request, RpcStreamConsumer consumer, RpcOptions options) {
            return null;
        }

        @Override
        public String destinationName() {
            return hostPort.toString();
        }

        @Override
        public String getAddressString() {
            return hostPort.toString();
        }

        @Override
        public ScheduledExecutorService executor() {
            return executor;
        }
    }
}
