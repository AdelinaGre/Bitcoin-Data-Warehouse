package com.datawarehouse.datawarehouse;

import com.datawarehouse.datawarehouse.dal.partitionKey.AssetKey;
import com.datawarehouse.datawarehouse.dal.repository.AssetRepository;
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
        "marketdata.extractor.mode=real"
})
class RealIngestionFlowIntegrationTest {

    @Autowired
    private MarketDataIngestionService marketDataIngestionService;

    @Autowired
    private AssetRepository assetRepository;

    @Test
    void shouldIngestRealBitfinexRecord() {
        IngestionResult result = marketDataIngestionService.ingest("ZRXUSD");

        assertNotNull(result);
        assertTrue(result.getFetchedRecords() > 0);
        assertTrue(result.getStoredRecords() >= 0);
        assertTrue(result.getSkippedRecords() >= 0);
        assertEquals(0, result.getFailedRecords());

        assertNotNull(assetRepository.findLatest(new AssetKey("ZRXUSD")));

        System.out.println("=== Real Ingestion Output ===");
        System.out.println(result);
    }
}
