package com.datawarehouse.datawarehouse.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.Map;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "data_sources")
public class DataSource {
    @Id
    private String id;
    private String name;
    private String description;
    private Instant systemDate;
    private String provider;
    private String dataset;
    private Map<String,String> requestContext;
    private Set<String> attributes;
}
