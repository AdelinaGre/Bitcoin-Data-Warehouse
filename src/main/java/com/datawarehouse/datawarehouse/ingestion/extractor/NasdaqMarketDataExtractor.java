package com.datawarehouse.datawarehouse.ingestion.extractor;

import com.datawarehouse.datawarehouse.ingestion.config.MarketDataApiProperties;
import com.datawarehouse.datawarehouse.ingestion.model.RawMarketDataPage;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name = "marketdata.extractor.mode", havingValue = "real")
public class NasdaqMarketDataExtractor implements MarketDataExtractor {

    private final RestClient restClient;
    private final MarketDataApiProperties properties;

    @Override
    public RawMarketDataPage fetchFirstPage(String assetIdentifier) {
        JsonNode response = restClient.get()
                .uri(uriBuilder -> {
                    var builder = uriBuilder
                            .scheme("https")
                            .host("data.nasdaq.com")
                            .path("/api/v3/datatables/{databaseCode}/{tableCode}.json")
                            .queryParam("code", assetIdentifier);

                    if (properties.getKey() != null && !properties.getKey().isBlank()) {
                        builder.queryParam("api_key", properties.getKey());
                    }

                    return builder.build(
                            properties.getDatabaseCode(),
                            properties.getTableCode()
                    );
                })
                .retrieve()
                .body(JsonNode.class);

        return toRawPage(response, assetIdentifier);
    }

    @Override
    public RawMarketDataPage fetchNextPage(String assetIdentifier, String nextCursor) {
        JsonNode response = restClient.get()
                .uri(uriBuilder -> {
                    var builder = uriBuilder
                            .scheme("https")
                            .host("data.nasdaq.com")
                            .path("/api/v3/datatables/{databaseCode}/{tableCode}.json")
                            .queryParam("code", assetIdentifier);

                    if (properties.getKey() != null && !properties.getKey().isBlank()) {
                        builder.queryParam("api_key", properties.getKey());
                    }

                    return builder.build(
                            properties.getDatabaseCode(),
                            properties.getTableCode()
                    );
                })
                .retrieve()
                .body(JsonNode.class);

        return toRawPage(response, assetIdentifier);
    }

    private RawMarketDataPage toRawPage(JsonNode response, String assetIdentifier) {
        JsonNode datatable = response.path("datatable");
        JsonNode columns = datatable.path("columns");
        JsonNode data = datatable.path("data");

        List<Map<String, Object>> records = new ArrayList<>();

        for (JsonNode row : data) {
            records.add(toRecord(columns, row));
        }

        String nextCursor = null;
        JsonNode meta = response.path("meta");
        if (meta.has("next_cursor_id") && !meta.get("next_cursor_id").isNull()) {
            nextCursor = meta.get("next_cursor_id").textValue();
        }

        return new RawMarketDataPage(
                records,
                nextCursor,
                nextCursor != null && !nextCursor.isBlank(),
                "NasdaqDataLink",
                properties.getTableCode()
        );
    }

    private Map<String, Object> toRecord(JsonNode columns, JsonNode row) {
        LinkedHashMap<String, Object> record = new LinkedHashMap<>();

        for (int i = 0; i < columns.size() && i < row.size(); i++) {
            JsonNode columnNode = columns.get(i);
            String columnName = columnNode.path("name").textValue();
            JsonNode valueNode = row.get(i);
            record.put(columnName, extractValue(valueNode));
        }

        return record;
    }

    private Object extractValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isNumber()) {
            return node.numberValue();
        }
        if (node.isBoolean()) {
            return node.booleanValue();
        }
        if (node.isTextual()) {
            return node.textValue();
        }
        return node.toString();
    }
}
