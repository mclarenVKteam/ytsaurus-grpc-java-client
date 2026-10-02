package tech.ytsaurus.client;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.Assert;
import org.junit.Test;

public class StaticProxyGetterTest {
    @Test
    public void testReturnsFixedProxyList() throws ExecutionException, InterruptedException {
        List<HostPort> proxies = List.of(
                HostPort.parse("pc.yt-rpc-proxy-adtech-profile.mesh.local:6000"),
                HostPort.parse("rc.yt-rpc-proxy-adtech-profile.mesh.local:6000"),
                HostPort.parse("kc.yt-rpc-proxy-adtech-profile.mesh.local:6000")
        );

        StaticProxyGetter getter = new StaticProxyGetter(proxies);
        CompletableFuture<List<HostPort>> future = getter.getProxies();

        Assert.assertTrue(future.isDone());
        Assert.assertFalse(future.isCompletedExceptionally());
        Assert.assertEquals(proxies, future.get());
    }

    @Test
    public void testReturnedListIsImmutable() {
        List<HostPort> proxies = List.of(HostPort.parse("localhost:9014"));
        StaticProxyGetter getter = new StaticProxyGetter(proxies);

        List<HostPort> result = getter.getProxies().join();
        Assert.assertThrows(UnsupportedOperationException.class, () -> result.add(HostPort.parse("localhost:9015")));
    }
}
