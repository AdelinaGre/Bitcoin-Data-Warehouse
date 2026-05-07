package com.datawarehouse.datawarehouse;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
@AutoConfigureTestRestTemplate

@Import(TestcontainersConfiguration.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class WarehouseWriteControllerTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Test
    void shouldIngestAssetThroughApi() {
        ResponseEntity<String> response = restTemplate.exchange(
                "/api/v1/ingestions/BTCUSD",
                HttpMethod.POST,
                HttpEntity.EMPTY,
                String.class
        );

        assertEquals(200, response.getStatusCode().value());
        assertTrue(response.getBody() != null && response.getBody().contains("\"storedRecords\":3"));
    }
}
