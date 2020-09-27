package com.db.dataplatform.techtest.service;

import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataLakeService;
import com.db.dataplatform.techtest.server.service.impl.DataLakeServiceImpl;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.time.Instant;

import static com.db.dataplatform.techtest.TestDataHelper.createTestDataBodyEntity;
import static com.db.dataplatform.techtest.TestDataHelper.createTestDataHeaderEntity;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DataLakeServiceTests {

    private DataLakeService testClass;

    @Mock
    private ObjectMapper objectMapper;

    @Mock
    private RestTemplate restTemplate;

    DataBodyEntity dataBodyEntity;

    @Before
    public void setup() {
        testClass = new DataLakeServiceImpl(objectMapper, restTemplate);
        DataHeaderEntity testDataHeaderEntity = createTestDataHeaderEntity(Instant.now());
        dataBodyEntity = createTestDataBodyEntity(testDataHeaderEntity);
    }

    @Test
    public void shouldPushDataToDataLakeAsExpected(){

        final ResponseEntity<DataBodyEntity> dataBodyEntityResponseEntity =
                new ResponseEntity<>(dataBodyEntity, HttpStatus.OK);

        //Given
        when(restTemplate.exchange(
                ArgumentMatchers.anyString(),
                ArgumentMatchers.any(HttpMethod.class),
                ArgumentMatchers.any(),
                ArgumentMatchers.<Class<DataBodyEntity>>any()
                )
        ).thenReturn(dataBodyEntityResponseEntity);

        //When
        testClass.pushDataToDataLake(dataBodyEntity);

        //then
        Mockito.verify(restTemplate, Mockito.times(1))
                .exchange(Mockito.anyString(),
                        Mockito.<HttpMethod> any(),
                        Mockito.<HttpEntity<?>> any(),
                        Mockito.<Class<?>> any());

    }

}
