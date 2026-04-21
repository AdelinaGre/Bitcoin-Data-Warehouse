package com.datawarehouse.datawarehouse.ingestion.extractor;

import com.datawarehouse.datawarehouse.ingestion.model.RawMarketDataPage;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
@ConditionalOnProperty(name = "marketdata.extractor.mode", havingValue = "stub", matchIfMissing = true)

public class StubMarketDataExtractor implements MarketDataExtractor {
    // simulates external data from provider and starts ingestion for one fake asset and return one fake page with one raw record
    @Override
    public RawMarketDataPage fetchFirstPage(String assetIdentifier) {
        Map<String, Object> record=Map.of(
              "symbol", assetIdentifier,
              "name", "Bitcoin US Dollar", "businessDate","2026-04-20T00:00:00Z",
                "close", 87500.50,
                "volume", 1520.0
        );
        return new RawMarketDataPage(
                List.of(record),
                null,
                false,
                "StubProvider",
                "stub-btc-dataset"
        );
    }

    //fetch r=the next page for the stub will return null because we don't have more
    @Override
    public RawMarketDataPage fetchNextPage(String assetIdentifier, String nextCursor) {
        return null;
    }
}
