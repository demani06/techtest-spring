package com.db.dataplatform.techtest.server.service;

import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;

public interface DataLakeService {
    void pushDataToDataLake(DataBodyEntity dataBody);
}
