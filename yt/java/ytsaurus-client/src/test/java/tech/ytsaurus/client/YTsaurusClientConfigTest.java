package tech.ytsaurus.client;

import org.junit.Assert;
import org.junit.Test;

public class YTsaurusClientConfigTest {
    @Test
    public void testDisableProxyDiscoveryDefaultIsFalse() {
        YTsaurusClientConfig config = YTsaurusClientConfig.builder().build();
        Assert.assertFalse(config.getDisableProxyDiscovery());
    }

    @Test
    public void testDisableProxyDiscoveryRoundTrip() {
        YTsaurusClientConfig config = YTsaurusClientConfig.builder()
                .setDisableProxyDiscovery(true)
                .build();

        Assert.assertTrue(config.getDisableProxyDiscovery());

        YTsaurusClientConfig rebuilt = config.toBuilder().build();
        Assert.assertTrue(rebuilt.getDisableProxyDiscovery());

        YTsaurusClientConfig disabled = rebuilt.toBuilder()
                .setDisableProxyDiscovery(false)
                .build();
        Assert.assertFalse(disabled.getDisableProxyDiscovery());
    }
}
