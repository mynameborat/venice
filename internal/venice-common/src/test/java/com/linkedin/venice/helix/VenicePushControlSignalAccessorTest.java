package com.linkedin.venice.helix;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.venice.pushmonitor.PushControlSignal;
import com.linkedin.venice.pushmonitor.PushControlSignalType;
import org.apache.helix.AccessOption;
import org.apache.helix.manager.zk.ZkBaseDataAccessor;
import org.apache.helix.zookeeper.zkclient.IZkDataListener;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class VenicePushControlSignalAccessorTest {
  private static final String CLUSTER_NAME = "testCluster";
  private static final String TOPIC = "testStore_v1";
  private static final String EXPECTED_PATH = "/" + CLUSTER_NAME + "/PushControlSignals/" + TOPIC;

  private ZkBaseDataAccessor<PushControlSignal> mockZkAccessor;
  private VenicePushControlSignalAccessor accessor;

  @SuppressWarnings("unchecked")
  @BeforeMethod
  public void setUp() {
    mockZkAccessor = mock(ZkBaseDataAccessor.class);
    accessor = new VenicePushControlSignalAccessor(CLUSTER_NAME, mockZkAccessor);
  }

  @Test
  public void testCreateAndGet() {
    PushControlSignal signal = new PushControlSignal(TOPIC);
    when(mockZkAccessor.create(eq(EXPECTED_PATH), eq(signal), anyInt())).thenReturn(true);

    accessor.createPushControlSignal(signal);
    verify(mockZkAccessor).create(eq(EXPECTED_PATH), eq(signal), eq(AccessOption.PERSISTENT));

    // Now test get
    PushControlSignal expected = new PushControlSignal(TOPIC);
    expected.emitSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE, 100L);
    when(mockZkAccessor.get(eq(EXPECTED_PATH), any(), eq(AccessOption.PERSISTENT))).thenReturn(expected);

    PushControlSignal result = accessor.getPushControlSignal(TOPIC);
    assertEquals(result.getKafkaTopic(), TOPIC);
    assertTrue(result.hasSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE));
    assertEquals(result.getSignalTimestamp(PushControlSignalType.BLOB_UPLOAD_COMPLETE), 100L);
  }

  @Test
  public void testUpdate() {
    PushControlSignal signal = new PushControlSignal(TOPIC);
    signal.emitSignal(PushControlSignalType.BLOB_UPLOAD_COMPLETE, 200L);
    when(mockZkAccessor.set(eq(EXPECTED_PATH), eq(signal), anyInt())).thenReturn(true);

    accessor.updatePushControlSignal(signal);
    verify(mockZkAccessor).set(eq(EXPECTED_PATH), eq(signal), eq(AccessOption.PERSISTENT));
  }

  @Test
  public void testDelete() {
    when(mockZkAccessor.remove(eq(EXPECTED_PATH), anyInt())).thenReturn(true);

    accessor.deletePushControlSignal(TOPIC);
    verify(mockZkAccessor).remove(eq(EXPECTED_PATH), eq(AccessOption.PERSISTENT));
  }

  @Test
  public void testGetNonExistent() {
    when(mockZkAccessor.get(eq(EXPECTED_PATH), any(), eq(AccessOption.PERSISTENT))).thenReturn(null);

    PushControlSignal result = accessor.getPushControlSignal(TOPIC);
    assertNull(result);
  }

  @Test
  public void testSubscribeAndUnsubscribe() {
    IZkDataListener listener = mock(IZkDataListener.class);

    accessor.subscribePushControlSignalChange(TOPIC, listener);
    verify(mockZkAccessor).subscribeDataChanges(eq(EXPECTED_PATH), eq(listener));

    accessor.unsubscribePushControlSignalChange(TOPIC, listener);
    verify(mockZkAccessor).unsubscribeDataChanges(eq(EXPECTED_PATH), eq(listener));
  }
}
