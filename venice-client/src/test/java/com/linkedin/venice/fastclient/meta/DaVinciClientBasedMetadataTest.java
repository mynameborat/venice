package com.linkedin.venice.fastclient.meta;

import com.linkedin.d2.balancer.D2Client;
import com.linkedin.davinci.client.DaVinciClient;
import com.linkedin.davinci.client.DaVinciConfig;
import com.linkedin.davinci.client.factory.CachingDaVinciClientFactory;
import com.linkedin.r2.transport.common.Client;
import com.linkedin.venice.common.VeniceSystemStoreType;
import com.linkedin.venice.controllerapi.VersionCreationResponse;
import com.linkedin.venice.fastclient.ClientConfig;
import com.linkedin.venice.fastclient.ClientTestUtils;
import com.linkedin.venice.integration.utils.D2TestUtils;
import com.linkedin.venice.integration.utils.ServiceFactory;
import com.linkedin.venice.integration.utils.VeniceClusterWrapper;
import com.linkedin.venice.integration.utils.VeniceRouterWrapper;
import com.linkedin.venice.meta.OnlineInstanceFinder;
import com.linkedin.venice.meta.ReadOnlySchemaRepository;
import com.linkedin.venice.meta.ReadOnlyStoreRepository;
import com.linkedin.venice.meta.Version;
import com.linkedin.venice.partitioner.DefaultVenicePartitioner;
import com.linkedin.venice.partitioner.VenicePartitioner;
import com.linkedin.venice.schema.SchemaEntry;
import com.linkedin.venice.serializer.RecordSerializer;
import com.linkedin.venice.serializer.SerializerDeserializerFactory;
import com.linkedin.venice.systemstore.schemas.StoreMetaKey;
import com.linkedin.venice.systemstore.schemas.StoreMetaValue;
import com.linkedin.venice.utils.PropertyBuilder;
import com.linkedin.venice.utils.TestUtils;
import com.linkedin.venice.utils.Time;
import com.linkedin.venice.utils.VeniceProperties;
import io.tehuti.metrics.MetricsRepository;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.commons.io.IOUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.linkedin.venice.ConfigKeys.*;
import static com.linkedin.venice.meta.PersistenceType.*;
import static org.testng.Assert.*;


public class DaVinciClientBasedMetadataTest {
  private static final int KEY_COUNT = 100;
  private static final long TIME_OUT = 60 * Time.MS_PER_SECOND;

  private final Schema.Parser parser = new Schema.Parser();
  private final VenicePartitioner defaultPartitioner = new DefaultVenicePartitioner();

  private VeniceClusterWrapper veniceCluster;
  private String storeName;
  private VeniceProperties daVinciBackendConfig;
  private DaVinciClientBasedMetadata daVinciClientBasedMetadata;
  private RecordSerializer<Object> keySerializer;
  private Client r2Client;
  private D2Client d2Client;
  private CachingDaVinciClientFactory daVinciClientFactory;
  private DaVinciClient<StoreMetaKey, StoreMetaValue> daVinciClientForMetaStore;

  @BeforeClass
  public void setup() throws Exception {
    veniceCluster = ServiceFactory.getVeniceCluster(1, 2, 1, 2, 100, true, false);
    r2Client = ClientTestUtils.getR2Client();
    d2Client = D2TestUtils.getAndStartD2Client(veniceCluster.getZk().getAddress());
    storeName = veniceCluster.createStore(KEY_COUNT);
    String metaSystemStoreName = VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName);
    veniceCluster.useControllerClient(controllerClient -> {
      VersionCreationResponse metaSystemStoreVersionCreationResponse = controllerClient.emptyPush(metaSystemStoreName,
          "test_bootstrap_meta_system_store", 10000);
      assertFalse(metaSystemStoreVersionCreationResponse.isError(),
          "New version creation for meta system store failed with error: " + metaSystemStoreVersionCreationResponse.getError());
      TestUtils.waitForNonDeterministicPushCompletion(metaSystemStoreVersionCreationResponse.getKafkaTopic(), controllerClient, 30,
          TimeUnit.SECONDS, Optional.empty());
      daVinciBackendConfig = new PropertyBuilder()
          .put(DATA_BASE_PATH, TestUtils.getTempDataDirectory().getAbsolutePath())
          .put(PERSISTENCE_TYPE, ROCKS_DB)
          .put(CLIENT_USE_SYSTEM_STORE_REPOSITORY, true)
          .put(CLIENT_META_SYSTEM_STORE_VERSION_MAP, storeName + ":" + metaSystemStoreVersionCreationResponse.getVersion())
          .build();
    });

    daVinciClientFactory = new CachingDaVinciClientFactory(d2Client,
        new MetricsRepository(), daVinciBackendConfig);
    daVinciClientForMetaStore = daVinciClientFactory.getAndStartSpecificAvroClient(
        VeniceSystemStoreType.META_STORE.getSystemStoreName(storeName),
        new DaVinciConfig(),
        StoreMetaValue.class);
    keySerializer = SerializerDeserializerFactory.getAvroGenericSerializer(parser.parse(VeniceClusterWrapper.DEFAULT_KEY_SCHEMA));

    // Populate required ClientConfig fields for initializing DaVinciClientBasedMetadata
    ClientConfig.ClientConfigBuilder clientConfigBuilder = new ClientConfig.ClientConfigBuilder<>();
    clientConfigBuilder.setStoreName(storeName);
    clientConfigBuilder.setR2Client(r2Client);
    clientConfigBuilder.setMetricsRepository(new MetricsRepository());
    clientConfigBuilder.setSpeculativeQueryEnabled(true);
    clientConfigBuilder.setVeniceZKAddress(veniceCluster.getZk().getAddress());
    clientConfigBuilder.setClusterName(veniceCluster.getClusterName());
    clientConfigBuilder.setDaVinciClientForMetaStore(daVinciClientForMetaStore);
    clientConfigBuilder.setMetadataRefreshInvervalInSeconds(1); // Faster refreshes for faster tests
    daVinciClientBasedMetadata = new DaVinciClientBasedMetadata(clientConfigBuilder.build());
    daVinciClientBasedMetadata.start();
  }

  @Test(timeOut = TIME_OUT)
  public void testMetadata() throws Exception {
    VeniceRouterWrapper routerWrapper = veniceCluster.getRandomVeniceRouter();
    ReadOnlyStoreRepository storeRepository = routerWrapper.getMetaDataRepository();
    OnlineInstanceFinder onlineInstanceFinder = routerWrapper.getOnlineInstanceFinder();
    assertEquals(daVinciClientBasedMetadata.getCurrentStoreVersion(), storeRepository.getStore(storeName).getCurrentVersion());
    List<Version> versions = storeRepository.getStore(storeName).getVersions();
    assertFalse(versions.isEmpty(), "Version list cannot be empty.");
    byte[] keyBytes = keySerializer.serialize(1);
    for (Version version : versions) {
      verifyMetadata(onlineInstanceFinder, version.getNumber(), version.getPartitionCount(), keyBytes);
    }
    // Make two new versions before checking the metadata again
    veniceCluster.createVersion(storeName, KEY_COUNT);
    veniceCluster.createVersion(storeName, KEY_COUNT);

    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () ->
        assertEquals(daVinciClientBasedMetadata.getCurrentStoreVersion(), storeRepository.getStore(storeName).getCurrentVersion()));
    versions = storeRepository.getStore(storeName).getVersions();
    assertFalse(versions.isEmpty(), "Version list cannot be empty.");
    for (Version version : versions) {
      verifyMetadata(onlineInstanceFinder, version.getNumber(), version.getPartitionCount(), keyBytes);
    }
  }

  @Test(timeOut = TIME_OUT)
  public void testMetadataSchemaRetriever() {
    ReadOnlySchemaRepository schemaRepository = veniceCluster.getRandomVeniceRouter().getSchemaRepository();
    assertEquals(daVinciClientBasedMetadata.getKeySchema(), schemaRepository.getKeySchema(storeName).getSchema());
    SchemaEntry latestValueSchema = schemaRepository.getLatestValueSchema(storeName);
    assertEquals(daVinciClientBasedMetadata.getLatestValueSchemaId().intValue(), latestValueSchema.getId());
    assertEquals(daVinciClientBasedMetadata.getLatestValueSchema(), latestValueSchema.getSchema());
    assertEquals(daVinciClientBasedMetadata.getValueSchema(latestValueSchema.getId()), latestValueSchema.getSchema());
    assertEquals(daVinciClientBasedMetadata.getValueSchemaId(latestValueSchema.getSchema()), latestValueSchema.getId());
  }

  private void verifyMetadata(OnlineInstanceFinder onlineInstanceFinder, int versionNumber, int partitionCount, byte[] key) {
    final String resourceName = Version.composeKafkaTopic(storeName, versionNumber);
    final int partitionId = ThreadLocalRandom.current().nextInt(0, partitionCount);
    TestUtils.waitForNonDeterministicAssertion(30, TimeUnit.SECONDS, () -> {
      assertEquals(defaultPartitioner.getPartitionId(key, partitionCount), daVinciClientBasedMetadata.getPartitionId(versionNumber, key));
      Set<String> routerReadyToServeView = onlineInstanceFinder.getReadyToServeInstances(resourceName, partitionId)
          .stream().map(instance -> instance.getUrl(true)).collect(Collectors.toSet());
      Set<String> metadataView = new HashSet<>(daVinciClientBasedMetadata.getReplicas(versionNumber, partitionId));
      assertEquals(metadataView.size(), routerReadyToServeView.size(),
          "Different number of ready to serve instances between router and StoreMetadata.");
      for (String instance : routerReadyToServeView) {
        assertTrue(metadataView.contains(instance), "Instance: " + instance + " is missing from StoreMetadata.");
      }
    });
  }

  @AfterClass
  public void cleanUp() {
    IOUtils.closeQuietly(daVinciClientBasedMetadata);
    IOUtils.closeQuietly(veniceCluster);
    if (r2Client != null) {
      r2Client.shutdown(null);
    }
    if (d2Client != null) {
      try {
        d2Client.shutdown(null);
      } catch (Exception e) {
        // Not sure why d2 shutdown could throw exception
      }
    }
    if (daVinciClientForMetaStore != null) {
      daVinciClientForMetaStore.close();
    }
    if (daVinciClientFactory != null) {
      daVinciClientFactory.close();
    }
  }

}