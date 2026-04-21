package com.datawarehouse.datawarehouse.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.Map;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "time_series_data")
public class TimeSeriesData {
    @Id
    private String id;
    private String assetId;
    private String dataSourceId;
    private Instant businessDate;
    private Instant systemDate;
    private Map<String, Object> payload;
    private boolean deleted;
    private Integer businessYear;
    private String provider;
    private String dataset;
    private Map<String,String> requestContext;




}
