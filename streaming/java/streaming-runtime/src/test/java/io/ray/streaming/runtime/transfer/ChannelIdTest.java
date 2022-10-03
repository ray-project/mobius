package io.ray.streaming.runtime.transfer;

import static org.testng.Assert.assertEquals;

import io.ray.streaming.common.utils.EnvUtil;
import io.ray.streaming.runtime.RayEnvBaseTest;
import io.ray.streaming.runtime.transfer.channel.ChannelId;
import org.testng.annotations.Test;

public class ChannelIdTest extends RayEnvBaseTest {

  static {
    EnvUtil.loadNativeLibraries();
  }

  @Test
  public void testIdStrToBytes() {
    String idStr = ChannelId.genRandomIdStr();
    assertEquals(idStr.length(), ChannelId.ID_LENGTH * 2);
    assertEquals(ChannelId.idStrToBytes(idStr).length, ChannelId.ID_LENGTH);
  }
}
