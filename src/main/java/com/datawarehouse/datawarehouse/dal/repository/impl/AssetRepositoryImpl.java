package com.datawarehouse.datawarehouse.dal.repository.impl;

import com.datawarehouse.datawarehouse.dal.partitionKey.AssetKey;
import com.datawarehouse.datawarehouse.dal.repository.AssetRepository;
import com.datawarehouse.datawarehouse.domain.Asset;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;



@Repository
@RequiredArgsConstructor
public class AssetRepositoryImpl implements AssetRepository {
    private final MongoTemplate mongoTemplate;

    @Override
    public Asset save(Asset entity){
        return mongoTemplate.save(entity);
    }

    @Override
    public void delete(Asset entity){
        mongoTemplate.remove(entity);
    }

    @Override
    public void deleteAll(AssetKey partitionKey) {
        Query query = new Query(Criteria.where("id").is(partitionKey.getId()));
        mongoTemplate.remove(query, Asset.class);
    }

    @Override
    public Asset findLatest(AssetKey partitionKey) {
        Query query=new Query(Criteria.where("id").is(partitionKey.getId()));
        query.with(Sort.by(Sort.Direction.DESC, "systemDate"));
        return mongoTemplate.findOne(query, Asset.class);
    }

    @Override
    public Iterable<Asset> findAll(AssetKey partitionKey) {
        Query query=new Query(Criteria.where("id").is(partitionKey.getId()));
        query.with(Sort.by(Sort.Direction.DESC, "systemDate"));
        return mongoTemplate.find(query, Asset.class);
    }
}
