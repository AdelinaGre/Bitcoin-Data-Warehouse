package com.datawarehouse.datawarehouse.config;

import com.datawarehouse.datawarehouse.ingestion.extractor.MarketDataExtractor;
import com.datawarehouse.datawarehouse.ingestion.model.RawMarketDataPage;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestClient;
import tools.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@Component
@RequiredArgsConstructor
@ConditionalOnProperty(name="marketdata.extractor.mode", havingValue = "real")
public class NasdaqMarketDataExtractor implements MarketDataExtractor {
    private final RestClient restClient;
    private final MarketDataApiProperties properties;
    @Override
    public RawMarketDataPage fetchFirstPage(String assetIdentifier) { // calles Nasdaq data link and downloads one daatset page
        String dataset= properties.getDataset(); //
        JsonNode response=restClient.get().uri(uriBuilder -> {var builder=uriBuilder
                .scheme("https")
                .host("data.nasdaq.com")
                .path("/api/v3/datasets/{dataset}/data.json")
                .queryParam("rows",10);
        if (properties.getKey()!=null && !properties.getKey().isBlank()){
            builder.queryParam("api_key",properties.getKey());
        }
        return builder.build(dataset);
        }).retrieve().body(JsonNode.class); // converts JSON to RawMarketDataPage
        return toRowPage(response,dataset);
    }

    private RawMarketDataPage toRowPage(JsonNode response, String dataset) { // converts JSON response into internal raw page model
        JsonNode datasetData=response.path("dataset_data");
        JsonNode columns=datasetData.path("column_names");
        JsonNode data=datasetData.path("data");
        List<Map<String,Object>> records=new ArrayList<>();
        for (JsonNode row:data){
            records.add(toRecord(columns, row));
        }

        return new RawMarketDataPage(
                records,
                null,
                false,
                "NasdaqDataLink",
                dataset
        );
    }

    @Override
    public RawMarketDataPage fetchNextPage(String assetIdentifier, String nextCursor) {
        return null;
    }
    // converts one JSON raw inta a Map<String,Object>
    private Map<String, Object> toRecord(JsonNode columns, JsonNode row) {
        java.util.LinkedHashMap<String, Object> record = new java.util.LinkedHashMap<>();

        for (int i = 0; i < columns.size() && i < row.size(); i++) {
            String columnName = columns.get(i).toString().replace("\"", "");
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
        if (node.isArray() || node.isObject()) {
            return node.toString();
        }
        return node.toString().replace("\"", "");
    }


}
