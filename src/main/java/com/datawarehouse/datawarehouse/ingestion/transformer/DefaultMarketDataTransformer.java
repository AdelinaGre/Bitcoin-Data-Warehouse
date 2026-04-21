package com.datawarehouse.datawarehouse.ingestion.transformer;

import com.datawarehouse.datawarehouse.domain.Asset;
import com.datawarehouse.datawarehouse.domain.DataSource;
import com.datawarehouse.datawarehouse.domain.TimeSeriesData;
import com.datawarehouse.datawarehouse.ingestion.model.CanonicalMarketData;
import com.datawarehouse.datawarehouse.ingestion.model.RawMarketDataPage;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Component
public class DefaultMarketDataTransformer implements MarketDataTransformer {

    @Override
    public CanonicalMarketData transform(String assetIdentifier, RawMarketDataPage rawPage) {
        Instant now = Instant.now();

        Set<String> attributeNames = rawPage.getRecords().stream()
                .flatMap(record -> record.keySet().stream())
                .collect(Collectors.toSet());

        Asset asset = new Asset(
                assetIdentifier,
                assetIdentifier,
                "Asset imported from Nasdaq Data Link",
                assetIdentifier,
                "CRYPTO",
                now,
                Map.of("provider", rawPage.getProvider())
        );

        DataSource dataSource = new DataSource(
                rawPage.getProvider() + "-" + rawPage.getDataset(),
                rawPage.getProvider(),
                "Imported from Nasdaq Data Link datatable API",
                now,
                rawPage.getProvider(),
                rawPage.getDataset(),
                Map.of("sourceType", "datatable"),
                attributeNames
        );

        List<TimeSeriesData> timeSeriesRecords = rawPage.getRecords().stream()
                .map(record -> safelyCreateRecord(assetIdentifier, dataSource, rawPage, record, now))
                .filter(java.util.Objects::nonNull)
                .toList();

        return new CanonicalMarketData(asset, dataSource, timeSeriesRecords);
    }

    private TimeSeriesData safelyCreateRecord(
            String assetIdentifier,
            DataSource dataSource,
            RawMarketDataPage rawPage,
            Map<String, Object> record,
            Instant now
    ) {
        try {
            return toTimeSeriesRecord(assetIdentifier, dataSource, rawPage, record, now);
        } catch (Exception ex) {
            return null;
        }
    }

    private TimeSeriesData toTimeSeriesRecord(
            String assetIdentifier,
            DataSource dataSource,
            RawMarketDataPage rawPage,
            Map<String, Object> record,
            Instant now
    ) {
        String businessDateRaw = findBusinessDateValue(record);
        Instant businessDate = LocalDate.parse(businessDateRaw).atStartOfDay().toInstant(ZoneOffset.UTC);

        Map<String, Object> payload = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : record.entrySet()) {
            String normalizedKey = normalizeKey(entry.getKey());
            if (!normalizedKey.equals("date") && !normalizedKey.equals("code")) {
                payload.put(normalizedKey, entry.getValue());
            }
        }

        return new TimeSeriesData(
                assetIdentifier + "-" + dataSource.getId() + "-" + businessDate,
                assetIdentifier,
                dataSource.getId(),
                businessDate,
                now,
                payload,
                false,
                businessDate.atZone(ZoneOffset.UTC).getYear(),
                rawPage.getProvider(),
                rawPage.getDataset(),
                Map.of("code", assetIdentifier)
        );
    }

    private String findBusinessDateValue(Map<String, Object> record) {
        for (String key : record.keySet()) {
            String normalized = normalizeKey(key);
            if (normalized.equals("date")) {
                Object value = record.get(key);
                if (value != null) {
                    return value.toString();
                }
            }
        }
        throw new IllegalArgumentException("No date column found");
    }

    private String normalizeKey(String key) {
        return key.trim().toLowerCase(Locale.ROOT).replace(" ", "_");
    }
}
