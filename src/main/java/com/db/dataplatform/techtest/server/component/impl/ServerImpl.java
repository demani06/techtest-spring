package com.db.dataplatform.techtest.server.component.impl;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import com.db.dataplatform.techtest.server.component.Server;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class ServerImpl implements Server {

    private final DataBodyService dataBodyServiceImpl;
    private final ModelMapper modelMapper;

    /**
     * @param envelope
     * @return true if there is a match with the client provided checksum.
     */
    @Override
    public boolean saveDataEnvelope(DataEnvelope envelope) {

        // Save to persistence.
        persist(envelope);

        log.info("Data persisted successfully, data name: {}", envelope.getDataHeader().getName());
        return true;
    }

    /**
     * @param blockType
     * @return list of persisted Data blocks
     */
    @Override
    public List<DataEnvelope> getDataEnvelopeListByBlockType(BlockTypeEnum blockType) {
        List<DataBodyEntity> dataBodyEntityList = dataBodyServiceImpl.getDataByBlockType(blockType);

        log.info("Data Entity list from repo with attribute name: {}", dataBodyEntityList);

        return getDataEnvelopesFromEntitiesList(dataBodyEntityList);

    }

    //TODO Possibly can be refactored to using Model map rather than set this manually
    private List<DataEnvelope> getDataEnvelopesFromEntitiesList(List<DataBodyEntity> dataBodyEntityList) {

        List<DataEnvelope> dataEnvelopeList = new ArrayList<>();
        for (DataBodyEntity dataBodyEntity : dataBodyEntityList) {
            DataEnvelope dataEnvelope = new DataEnvelope();
            dataEnvelope.setDataBody(new DataBody(dataBodyEntity.getDataBody()));
            dataEnvelope.setDataHeader(new DataHeader(dataBodyEntity.getDataHeaderEntity().getName(),dataBodyEntity.getDataHeaderEntity().getBlocktype()));
            dataEnvelopeList.add(dataEnvelope);
        }
        return dataEnvelopeList;
    }

    private void persist(DataEnvelope envelope) {
        log.info("Persisting data with attribute name: {}", envelope.getDataHeader().getName());
        DataHeaderEntity dataHeaderEntity = modelMapper.map(envelope.getDataHeader(), DataHeaderEntity.class);

        DataBodyEntity dataBodyEntity = modelMapper.map(envelope.getDataBody(), DataBodyEntity.class);
        dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);

        saveData(dataBodyEntity);
    }

    private void saveData(DataBodyEntity dataBodyEntity) {
        dataBodyServiceImpl.saveDataBody(dataBodyEntity);
    }

}
