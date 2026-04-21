package com.datawarehouse.datawarehouse.dal.repository;

import com.datawarehouse.datawarehouse.dal.partitionKey.AssetKey;
import com.datawarehouse.datawarehouse.domain.Asset;

public interface AssetRepository extends WarehouseRepository<Asset, AssetKey> {
    //This is the DAL contract for asset persistence
}
