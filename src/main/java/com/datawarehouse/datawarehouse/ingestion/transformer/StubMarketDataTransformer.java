package com.datawarehouse.datawarehouse.ingestion.transformer;

import com.datawarehouse.datawarehouse.domain.Asset;
import com.datawarehouse.datawarehouse.domain.DataSource;
import com.datawarehouse.datawarehouse.domain.TimeSeriesData;
import com.datawarehouse.datawarehouse.ingestion.model.CanonicalMarketData;
import com.datawarehouse.datawarehouse.ingestion.model.RawMarketDataPage;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.time.ZonedDateTime;
import java.util.Set;

@Component
public class StubMarketDataTransformer implements MarketDataTransformer{
    @Override
    public CanonicalMarketData transform(String assetIdentifier, RawMarketDataPage rawPage) {
        Map<String,Object> record=rawPage.getRecords().getFirst();
        Instant now=Instant.now();

        Asset asset=new Asset(
                assetIdentifier,
                (String) record.get("name"),
                "Stub asset created from ingestion flow",
                assetIdentifier,
                "CRYPTO",
                now,
                Map.of("source", rawPage.getProvider())
        );

        DataSource dataSource=new DataSource(
                rawPage.getProvider() + "-" + rawPage.getDataset(),
                rawPage.getProvider(),
                "Stub market data source",
                now,
                rawPage.getProvider(),
                rawPage.getDataset(),
                Map.of("mode", "stub"),
                Set.of("close", "volume")
        );
        Instant businessDate = ZonedDateTime.parse((String) record.get("businessDate")).toInstant();

        TimeSeriesData timeSeriesData = new TimeSeriesData(
                assetIdentifier + "-" + rawPage.getProvider() + "-" + businessDate,
                assetIdentifier,
                dataSource.getId(),
                businessDate,
                now,
                Map.of(
                        "close", record.get("close"),
                        "volume", record.get("volume")
                ),
                false,
                businessDate.atZone(java.time.ZoneOffset.UTC).getYear(),
                rawPage.getProvider(),
                rawPage.getDataset(),
                Map.of("mode", "stub")
        );

        return new CanonicalMarketData(
                asset,
                dataSource,
                List.of(timeSeriesData)
        );
    }
}
