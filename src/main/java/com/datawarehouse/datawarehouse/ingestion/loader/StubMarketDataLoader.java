package com.datawarehouse.datawarehouse.ingestion.loader;

import com.datawarehouse.datawarehouse.dal.partitionKey.AssetKey;
import com.datawarehouse.datawarehouse.dal.partitionKey.DataSourceKey;
import com.datawarehouse.datawarehouse.dal.repository.AssetRepository;
import com.datawarehouse.datawarehouse.dal.repository.DataSourceRepository;
import com.datawarehouse.datawarehouse.dal.repository.TimeSeriesDataRepository;
import com.datawarehouse.datawarehouse.domain.TimeSeriesData;
import com.datawarehouse.datawarehouse.ingestion.model.CanonicalMarketData;
import com.datawarehouse.datawarehouse.ingestion.model.IngestionResult;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class StubMarketDataLoader implements MarketDataLoader{
    private final AssetRepository assetRepository;
    private final DataSourceRepository dataSourceRepository;
    private final TimeSeriesDataRepository timeSeriesDataRepository;


    @Override
    public IngestionResult load(CanonicalMarketData canonicalMarketData) {
        int stored=0;
        int skipped =0;
        int failed=0;

        try{
            if(assetRepository.findLatest(new AssetKey(canonicalMarketData.getAsset().getId()))==null){
                assetRepository.save(canonicalMarketData.getAsset());
                stored++;
            }else{
                skipped++;
            }
            if (dataSourceRepository.findLatest(new DataSourceKey(canonicalMarketData.getDataSource().getId())) == null) {
                dataSourceRepository.save(canonicalMarketData.getDataSource());
                stored++;
            } else {
                skipped++;
            }

            for (TimeSeriesData record : canonicalMarketData.getTimeSeriesRecords()) {
                timeSeriesDataRepository.save(record);
                stored++;
            }
        }catch(Exception e){
            failed++;
        }


        return new IngestionResult(
                canonicalMarketData.getTimeSeriesRecords().size(),
                canonicalMarketData.getTimeSeriesRecords().size(),
                stored,
                skipped,
                failed
        );
    }
}
