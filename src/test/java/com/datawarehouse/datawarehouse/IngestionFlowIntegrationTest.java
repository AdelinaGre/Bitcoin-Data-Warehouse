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
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.*;

@Import(TestcontainersConfiguration.class)
@SpringBootTest
@TestPropertySource(properties = {
        "marketdata.extractor.mode=stub"
})
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

        String expectedDataSourceId = "stub-btc-dataset";
        String expectedAssetId = "stub-btc-dataset/BTCUSD";

        Asset asset = assetRepository.findLatest(new AssetKey(expectedAssetId));
        assertNotNull(asset);
        assertEquals(expectedAssetId, asset.getId());

        DataSource dataSource = dataSourceRepository.findLatest(new DataSourceKey(expectedDataSourceId));
        assertNotNull(dataSource);
        assertEquals(expectedDataSourceId, dataSource.getId());

        TimeSeriesData timeSeriesData = timeSeriesDataRepository.findLatest(
                new TimeSeriesPartitionKey(expectedAssetId, expectedDataSourceId, 2026)
        );
        assertNotNull(timeSeriesData);
        assertEquals(expectedAssetId, timeSeriesData.getAssetId());
        assertFalse(timeSeriesData.isDeleted());
        assertTrue(timeSeriesData.getPayload().containsKey("close"));
    }
}
