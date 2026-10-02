package tech.ytsaurus.client.rpc;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class GrpcMessageUtilsTest {
    @Test
    public void testPackAndSplitMessage() {
        byte[] body = "proto-body".getBytes();
        byte[] a1 = "rowset-a".getBytes();
        byte[] a2 = null;
        byte[] a3 = "rowset-b".getBytes();

        byte[] packed = GrpcMessageUtils.packMessage(body, Arrays.asList(a1, a2, a3));
        GrpcMessageUtils.MessageWithAttachments result = GrpcMessageUtils.splitMessage(packed, body.length);

        Assert.assertArrayEquals(body, result.getBody());
        Assert.assertEquals(3, result.getAttachments().size());
        Assert.assertArrayEquals(a1, result.getAttachments().get(0));
        Assert.assertNull(result.getAttachments().get(1));
        Assert.assertArrayEquals(a3, result.getAttachments().get(2));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBodySizeOutsidePayload() {
        byte[] packed = GrpcMessageUtils.packMessage("body".getBytes(), List.of());
        GrpcMessageUtils.splitMessage(packed, packed.length + 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testTruncatedAttachment() {
        byte[] packed = GrpcMessageUtils.packMessage("body".getBytes(), List.of("short".getBytes()));
        // Layout: body (4 bytes) | attachment length prefix (4 bytes) | attachment data (5 bytes).
        // Corrupt the length prefix at positions 4..7 to point beyond buffer.
        packed[4] = (byte) 0xFF;
        packed[5] = 0;
        packed[6] = 0;
        packed[7] = 0;
        GrpcMessageUtils.splitMessage(packed, 4);
    }
}
