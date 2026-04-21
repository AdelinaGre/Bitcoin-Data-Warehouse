package com.datawarehouse.datawarehouse;

import com.datawarehouse.datawarehouse.dal.partitionKey.AssetKey;
import com.datawarehouse.datawarehouse.dal.partitionKey.DataSourceKey;
import com.datawarehouse.datawarehouse.dal.partitionKey.TimeSeriesPartitionKey;
import com.datawarehouse.datawarehouse.dal.repository.AssetRepository;
import com.datawarehouse.datawarehouse.dal.repository.DataSourceRepository;
import com.datawarehouse.datawarehouse.dal.repository.TimeSeriesDataRepository;
import com.datawarehouse.datawarehouse.domain.Asset;
import com.datawarehouse.datawarehouse.domain.DataSource;
import com.datawarehouse.datawarehouse.domain.TimeSeriesData;
import com.datawarehouse.datawarehouse.ingestion.MarketDataIngestionService;
import com.datawarehouse.datawarehouse.ingestion.model.IngestionResult;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

import static org.junit.jupiter.api.Assertions.*;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
class IngestionFlowIntegrationTest {

    @Autowired
    private MarketDataIngestionService marketDataIngestionService;

    @Autowired
    private AssetRepository assetRepository;

    @Autowired
    private DataSourceRepository dataSourceRepository;

    @Autowired
    private TimeSeriesDataRepository timeSeriesDataRepository;

    @Test
    void shouldIngestFakeRecordEndToEnd() {
        IngestionResult result = marketDataIngestionService.ingest("BTCUSD");

        assertNotNull(result);
        assertEquals(1, result.getFetchedRecords());
        assertEquals(1, result.getTransformedRecords());
        assertEquals(3, result.getStoredRecords());
        assertEquals(0, result.getFailedRecords());

        Asset asset = assetRepository.findLatest(new AssetKey("BTCUSD"));
        assertNotNull(asset);
        assertEquals("BTCUSD", asset.getId());

        DataSource dataSource = dataSourceRepository.findLatest(new DataSourceKey("StubProvider-stub-btc-dataset"));
        assertNotNull(dataSource);
        assertEquals("StubProvider", dataSource.getProvider());

        TimeSeriesData timeSeriesData = timeSeriesDataRepository.findLatest(
                new TimeSeriesPartitionKey("BTCUSD", "StubProvider-stub-btc-dataset", 2026)
        );
        assertNotNull(timeSeriesData);
        assertEquals("BTCUSD", timeSeriesData.getAssetId());
        assertFalse(timeSeriesData.isDeleted());
        assertTrue(timeSeriesData.getPayload().containsKey("close"));

        System.out.println("=== Ingestion Demo Output ===");
        System.out.println("Ingestion result: " + result);
        System.out.println("Stored asset: " + asset);
        System.out.println("Stored data source: " + dataSource);
        System.out.println("Stored time series record: " + timeSeriesData);
    }
}
