package com.datawarehouse.datawarehouse;

import com.datawarehouse.datawarehouse.domain.Asset;
import com.datawarehouse.datawarehouse.domain.DataSource;
import com.datawarehouse.datawarehouse.domain.TimeSeriesData;
import com.datawarehouse.datawarehouse.dal.repository.AssetRepository;
import com.datawarehouse.datawarehouse.dal.repository.DataSourceRepository;
import com.datawarehouse.datawarehouse.dal.repository.TimeSeriesDataRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
@AutoConfigureTestRestTemplate

@Import(TestcontainersConfiguration.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@TestPropertySource(properties = {
        "marketdata.extractor.mode=stub"
})
class WarehouseReadControllerTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private AssetRepository assetRepository;

    @Autowired
    private DataSourceRepository dataSourceRepository;

    @Autowired
    private TimeSeriesDataRepository timeSeriesDataRepository;

    private final String assetId = "QDL/BITFINEX/ZRXUSD";
    private final String dataSourceId = "QDL/BITFINEX";

    @BeforeEach
    void setUp() {
        Asset asset = new Asset(
                assetId,
                "ZRXUSD",
                "Test asset",
                "ZRXUSD",
                "CRYPTO",
                Instant.now(),
                Map.of("provider", "NasdaqDataLink")
        );
        assetRepository.save(asset);

        DataSource dataSource = new DataSource(
                dataSourceId,
                "NasdaqDataLink",
                "Test source",
                Instant.now(),
                "NasdaqDataLink",
                dataSourceId,
                Map.of("sourceType", "datatable"),
                Set.of("close", "volume")
        );
        dataSourceRepository.save(dataSource);

        Instant d1 = LocalDate.parse("2025-01-01").atStartOfDay().toInstant(ZoneOffset.UTC);
        Instant d2 = LocalDate.parse("2025-01-02").atStartOfDay().toInstant(ZoneOffset.UTC);

        timeSeriesDataRepository.save(new TimeSeriesData(
                assetId + "-" + d1 + "-v1",
                assetId,
                dataSourceId,
                d1,
                Instant.parse("2025-01-01T01:00:00Z"),
                Map.of("close", 10.0),
                false,
                2025,
                "NasdaqDataLink",
                dataSourceId,
                Map.of("code", "ZRXUSD")
        ));

        timeSeriesDataRepository.save(new TimeSeriesData(
                assetId + "-" + d1 + "-v2",
                assetId,
                dataSourceId,
                d1,
                Instant.parse("2025-01-01T02:00:00Z"),
                Map.of("close", 11.0),
                false,
                2025,
                "NasdaqDataLink",
                dataSourceId,
                Map.of("code", "ZRXUSD")
        ));

        timeSeriesDataRepository.save(new TimeSeriesData(
                assetId + "-" + d2,
                assetId,
                dataSourceId,
                d2,
                Instant.parse("2025-01-02T01:00:00Z"),
                Map.of("close", 20.0, "volume", 1000),
                false,
                2025,
                "NasdaqDataLink",
                dataSourceId,
                Map.of("code", "ZRXUSD")
        ));
    }

    @Test
    void shouldReturnPagedAssets() {
        ResponseEntity<String> response = restTemplate.getForEntity(
                "/api/v1/assets?offset=0&limit=10",
                String.class
        );

        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody().contains(assetId));
        assertTrue(response.getBody().contains("\"hasNext\":false"));
    }

    @Test
    void shouldReturnLatestVersionPerBusinessDateInDescendingOrder() {
        ResponseEntity<String> response = restTemplate.getForEntity(
                "/api/v1/data?assetId=" + assetId +
                        "&dataSourceId=" + dataSourceId +
                        "&startBusinessDate=2025-01-01" +
                        "&endBusinessDate=2025-01-03" +
                        "&includeAttributes=false&offset=0&limit=10",
                String.class
        );

        assertEquals(200, response.getStatusCode().value());

        String body = response.getBody();
        assertTrue(body.contains("\"close\":20.0"));
        assertTrue(body.contains("\"close\":11.0"));
        assertTrue(!body.contains("\"close\":10.0"));
        assertTrue(body.indexOf("\"close\":20.0") < body.indexOf("\"close\":11.0"));
    }

    @Test
    void shouldIncludeAttributesWhenRequested() {
        ResponseEntity<String> response = restTemplate.getForEntity(
                "/api/v1/data?assetId=" + assetId +
                        "&dataSourceId=" + dataSourceId +
                        "&startBusinessDate=2025-01-01" +
                        "&endBusinessDate=2025-01-03" +
                        "&includeAttributes=true&offset=0&limit=10",
                String.class
        );

        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody().contains("close"));
        assertTrue(response.getBody().contains("volume"));
    }

    @Test
    void shouldSupportPaginationOnDataEndpoint() {
        ResponseEntity<String> response = restTemplate.getForEntity(
                "/api/v1/data?assetId=" + assetId +
                        "&dataSourceId=" + dataSourceId +
                        "&startBusinessDate=2025-01-01" +
                        "&endBusinessDate=2025-01-03" +
                        "&includeAttributes=false&offset=0&limit=1",
                String.class
        );

        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody().contains("\"hasNext\":true"));
    }
}
