package com.db.dataplatform.techtest.server.service.impl;

import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.repository.DataStoreRepository;
import com.db.dataplatform.techtest.server.service.DataLakeService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import static com.db.dataplatform.techtest.server.utils.Utils.getHttpHeaders;

@Service
@RequiredArgsConstructor
@Slf4j
public class DataLakeServiceImpl implements DataLakeService {

    private final DataStoreRepository dataStoreRepository;
    @Autowired
    private ObjectMapper objectMapper;
    @Autowired
    private RestTemplate restTemplate;

    private final String DATA_LAKE_URL = "http://localhost:8090/hadoopserver/pushbigdata";

    @Override
    @Async
    public void pushDataToDataLake(DataBodyEntity dataBody) {

        String requestBodyString = getEntityAsJsonString(dataBody);

        log.info("Trying to push Data {} to DataLake, DataLakeURL= {}",requestBodyString, DATA_LAKE_URL);

        HttpEntity<String> httpEntity = new HttpEntity<>(requestBodyString, getHttpHeaders());
        //Fire and Forget since the timeouts are expected.
        //In a Production world, this ideally would be an Async request to a Pub-Sub/Queue which will enable resilence
        //Also the circuit breakers like Hystrix or at a service mesh can be used
        restTemplate.exchange(DATA_LAKE_URL, HttpMethod.POST, httpEntity, HttpStatus.class);

    }

    private String getEntityAsJsonString(DataBodyEntity dataBody) {
        String requestBodyString = null;
        try {
            requestBodyString = objectMapper.writeValueAsString(dataBody);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return requestBodyString;
    }
}
