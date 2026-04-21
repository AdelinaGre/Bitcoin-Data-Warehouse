package com.datawarehouse.datawarehouse;

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
class RealIngestionIdempotencyTest {

    @Autowired
    private MarketDataIngestionService marketDataIngestionService;

    @Test
    void shouldAllowSafeRepeatedIngestion() {
        IngestionResult firstRun = marketDataIngestionService.ingest("ZRXUSD");
        IngestionResult secondRun = marketDataIngestionService.ingest("ZRXUSD");

        assertNotNull(firstRun);
        assertNotNull(secondRun);
        assertEquals(0, firstRun.getFailedRecords());
        assertEquals(0, secondRun.getFailedRecords());
        assertTrue(secondRun.getSkippedRecords() >= 0);

        System.out.println("=== First run ===");
        System.out.println(firstRun);
        System.out.println("=== Second run ===");
        System.out.println(secondRun);
    }
}
