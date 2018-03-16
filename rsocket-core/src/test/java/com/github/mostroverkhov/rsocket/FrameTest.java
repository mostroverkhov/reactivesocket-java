package com.github.mostroverkhov.rsocket;

import static org.junit.Assert.assertEquals;

import com.github.mostroverkhov.rsocket.frame.FrameHeaderFlyweight;
import com.github.mostroverkhov.rsocket.util.PayloadImpl;
import org.junit.Test;

public class FrameTest {
  @Test
  public void testFrameToString() {
    final Frame requestFrame =
        Frame.Request.from(1, FrameType.REQUEST_RESPONSE, new PayloadImpl("streaming in -> 0"), 1);
    assertEquals(
        "Frame => Stream ID: 1 Type: REQUEST_RESPONSE Payload: data: \"streaming in -> 0\" ",
        requestFrame.toString());
  }

  @Test
  public void testFrameWithMetadataToString() {
    final Frame requestFrame =
        Frame.Request.from(
            1, FrameType.REQUEST_RESPONSE, new PayloadImpl("streaming in -> 0", "metadata"), 1);
    assertEquals(
        "Frame => Stream ID: 1 Type: REQUEST_RESPONSE Payload: metadata: \"metadata\" data: \"streaming in -> 0\" ",
        requestFrame.toString());
  }

  @Test
  public void testPayload() {
    Frame frame =
        Frame.PayloadFrame.from(
            1, FrameType.NEXT_COMPLETE, new PayloadImpl("Hello"), FrameHeaderFlyweight.FLAGS_C);
    frame.toString();
  }
}
