package com.datawarehouse.datawarehouse.dal.repository.impl;

import com.datawarehouse.datawarehouse.dal.partitionKey.TimeSeriesPartitionKey;
import com.datawarehouse.datawarehouse.dal.repository.TimeSeriesDataRepository;
import com.datawarehouse.datawarehouse.domain.TimeSeriesData;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Repository
@RequiredArgsConstructor
public class TimeSeriesDataRepositoryImpl implements TimeSeriesDataRepository {

    private final MongoTemplate mongoTemplate;

    @Override
    public TimeSeriesData save(TimeSeriesData entity) {
        return mongoTemplate.save(entity);
    }

    @Override
    public void delete(TimeSeriesData entity) {
        mongoTemplate.remove(entity);
    }

    @Override
    public void deleteAll(TimeSeriesPartitionKey partitionKey) {
        Query query = new Query(buildPartitionCriteria(partitionKey));
        mongoTemplate.remove(query, TimeSeriesData.class);
    }

    @Override
    public TimeSeriesData findLatest(TimeSeriesPartitionKey partitionKey) {
        Query query = new Query(buildPartitionCriteria(partitionKey));
        query.with(Sort.by(Sort.Direction.DESC, "systemDate"));
        return mongoTemplate.findOne(query, TimeSeriesData.class);
    }

    @Override
    public Iterable<TimeSeriesData> findAll(TimeSeriesPartitionKey partitionKey) {
        Query query = new Query(buildPartitionCriteria(partitionKey));
        query.with(Sort.by(
                Sort.Order.asc("businessDate"),
                Sort.Order.desc("systemDate")
        ));
        return mongoTemplate.find(query, TimeSeriesData.class);
    }

    @Override
    public Iterable<TimeSeriesData> findByBusinessDateRange(TimeSeriesPartitionKey key, Instant from, Instant to) {
        List<Criteria> criteriaList = new ArrayList<>();
        criteriaList.add(buildPartitionCriteria(key));
        criteriaList.add(Criteria.where("businessDate").gte(from).lte(to));

        Query query = new Query(new Criteria().andOperator(criteriaList.toArray(new Criteria[0])));
        query.with(Sort.by(
                Sort.Order.asc("businessDate"),
                Sort.Order.desc("systemDate")
        ));

        return mongoTemplate.find(query, TimeSeriesData.class);
    }

    @Override
    public Iterable<TimeSeriesData> findByBusinessDate(TimeSeriesPartitionKey key, Instant businessDate) {
        List<Criteria> criteriaList = new ArrayList<>();
        criteriaList.add(buildPartitionCriteria(key));
        criteriaList.add(Criteria.where("businessDate").is(businessDate));

        Query query = new Query(new Criteria().andOperator(criteriaList.toArray(new Criteria[0])));
        query.with(Sort.by(Sort.Direction.DESC, "systemDate"));

        return mongoTemplate.find(query, TimeSeriesData.class);
    }

    private Criteria buildPartitionCriteria(TimeSeriesPartitionKey key) {
        List<Criteria> criteriaList = new ArrayList<>();
        criteriaList.add(Criteria.where("assetId").is(key.getAssetId()));
        criteriaList.add(Criteria.where("dataSourceId").is(key.getDataSourceId()));

        if (key.getBusinessYear() != null) {
            criteriaList.add(Criteria.where("businessYear").is(key.getBusinessYear()));
        }

        return new Criteria().andOperator(criteriaList.toArray(new Criteria[0]));
    }
}
