package com.db.dataplatform.techtest.client.component.impl;

import com.db.dataplatform.techtest.client.api.model.DataEnvelope;
import com.db.dataplatform.techtest.client.component.Client;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.db.dataplatform.techtest.server.utils.Utils.getHttpHeaders;
import static java.util.Objects.nonNull;

/**
 * Client code does not require any test coverage
 */

@Service
@Slf4j
@RequiredArgsConstructor
public class ClientImpl implements Client {

    public static final String URI_PUSHDATA = "http://localhost:8090/dataserver/pushdata";
    public static final UriTemplate URI_GETDATA = new UriTemplate("http://localhost:8090/dataserver/data/{blockType}");
    public static final UriTemplate URI_PATCHDATA = new UriTemplate("http://localhost:8090/dataserver/update/{name}/{newBlockType}");

    @Autowired
    private RestTemplate restTemplate;

    @Override
    public void pushData(DataEnvelope dataEnvelope) {
        log.info("Pushing data {} to {}", dataEnvelope.getDataHeader().getName(), URI_PUSHDATA);
        final HttpHeaders httpHeaders = getHttpHeaders();
        httpHeaders.add("checkSum", "E96FE4914DC94E9AAF2FABB3B4E63366");

        HttpEntity<DataEnvelope> httpEntity = new HttpEntity<>(dataEnvelope, httpHeaders);
        ResponseEntity<Boolean> responseEntity = restTemplate.exchange(URI_PUSHDATA, HttpMethod.POST, httpEntity, Boolean.class);

        log.info("Push Data Response returned value: {}", responseEntity.getBody());

    }

    @Override
    public List<DataEnvelope> getData(String blockType) {
        log.info("Query for data with header block type {}", blockType);

        HttpEntity<DataEnvelope> httpEntity = new HttpEntity<>(getHttpHeaders());

        final String url = URI_GETDATA.toString();

        Map<String, String> params = new HashMap<>();
        params.put("blockType", blockType);
        ResponseEntity<List<DataEnvelope>> responseEntity = restTemplate.exchange(url, HttpMethod.GET, httpEntity, new ParameterizedTypeReference<List<DataEnvelope>>() {
        }, params);

        log.info("Response entity list for data from GET Call {}", responseEntity.getBody());

        return responseEntity.getBody();
    }

    @Override
    public boolean updateData(String blockName, String newBlockType) {
        log.info("Updating blockType to {} for block with name {}", newBlockType, blockName);

        HttpEntity<Boolean> httpEntity = new HttpEntity<>(getHttpHeaders());

        final String url = URI_PATCHDATA.toString();

        Map<String, String> params = new HashMap<>();
        params.put("name", blockName);
        params.put("newBlockType", newBlockType);
        ResponseEntity<Boolean> responseEntity = restTemplate.exchange(url, HttpMethod.PATCH, httpEntity, Boolean.class, params);

        boolean returnValue = false;
        if(nonNull(responseEntity.getBody())){
            returnValue = true;
        }

        log.info("Response after patching the data for block name {} and block type {},  return value: {}",blockName, newBlockType, returnValue);

        return returnValue;
    }

}
