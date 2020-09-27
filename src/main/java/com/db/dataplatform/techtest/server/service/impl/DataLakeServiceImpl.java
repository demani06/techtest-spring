package com.db.dataplatform.techtest.server.service.impl;

import com.db.dataplatform.techtest.server.exception.HadoopClientException;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.service.DataLakeService;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.retry.annotation.Retryable;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import static com.db.dataplatform.techtest.Constant.DATA_LAKE_URL;
import static com.db.dataplatform.techtest.server.utils.Utils.getHttpHeaders;

/*
* The push data to DataLake method is annotated with @Async so that it completes in another thread and it makes the client call non blocking
* Also @Retryable is used for handling timeouts and in the below scenario it is tried 5 times
* */

@Service
@AllArgsConstructor
@Slf4j
public class DataLakeServiceImpl implements DataLakeService {

    private final ObjectMapper objectMapper;
    private final RestTemplate restTemplate;

    @SneakyThrows
    @Override
    @Async
    @Retryable(maxAttempts = 5, value = HadoopClientException.class)
    public void pushDataToDataLake(DataBodyEntity dataBody) {

        String requestBodyString = getEntityAsJsonString(dataBody);

        log.info("Trying to push Data to DataLake, DataLakeURL= {}, requestString={}", DATA_LAKE_URL, requestBodyString);

        HttpEntity<String> httpEntity = new HttpEntity<>(requestBodyString, getHttpHeaders());
        //In a Production world, this ideally would be an Async request to a Pub-Sub/Queue which will enable resilence
        //Also the circuit breakers like Hystrix or at a service mesh would be used in an Ideal World
        try {
            restTemplate.exchange(DATA_LAKE_URL, HttpMethod.POST, httpEntity, HttpStatus.class);
        } catch (Exception e) {
            throw new HadoopClientException("Timeout exception");
        }
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
