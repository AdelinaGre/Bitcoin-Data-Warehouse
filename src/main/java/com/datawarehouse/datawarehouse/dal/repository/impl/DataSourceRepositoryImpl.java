package com.datawarehouse.datawarehouse.dal.repository.impl;

import com.datawarehouse.datawarehouse.dal.partitionKey.AssetKey;
import com.datawarehouse.datawarehouse.dal.partitionKey.DataSourceKey;
import com.datawarehouse.datawarehouse.dal.repository.DataSourceRepository;
import com.datawarehouse.datawarehouse.domain.Asset;
import com.datawarehouse.datawarehouse.domain.DataSource;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

@Repository
@RequiredArgsConstructor
public class DataSourceRepositoryImpl implements DataSourceRepository {
    private final MongoTemplate mongoTemplate;

    @Override
    public DataSource save(DataSource entity){
        return mongoTemplate.save(entity);
    }

    @Override
    public void delete(DataSource entity){
        mongoTemplate.remove(entity);
    }

    @Override
    public void deleteAll(DataSourceKey partitionKey) {
        Query query = new Query(Criteria.where("id").is(partitionKey.getId()));
        mongoTemplate.remove(query, DataSource.class);
    }

    @Override
    public DataSource findLatest(DataSourceKey partitionKey) {
        Query query=new Query(Criteria.where("id").is(partitionKey.getId()));
        query.with(Sort.by(Sort.Direction.DESC, "systemDate"));
        return mongoTemplate.findOne(query, DataSource.class);
    }

    @Override
    public Iterable<DataSource> findAll(DataSourceKey partitionKey) {
        Query query=new Query(Criteria.where("id").is(partitionKey.getId()));
        query.with(Sort.by(Sort.Direction.DESC, "systemDate"));
        return mongoTemplate.find(query, DataSource.class);
    }
}
