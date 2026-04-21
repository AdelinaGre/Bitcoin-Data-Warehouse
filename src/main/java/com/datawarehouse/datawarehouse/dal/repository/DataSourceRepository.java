package com.datawarehouse.datawarehouse.dal.repository;

import com.datawarehouse.datawarehouse.dal.partitionKey.DataSourceKey;
import com.datawarehouse.datawarehouse.domain.DataSource;

public interface DataSourceRepository extends WarehouseRepository<DataSource, DataSourceKey> {
    //This is the DAL contract for provider/source persistence
}
