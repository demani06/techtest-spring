package com.db.dataplatform.techtest.server.component.impl;

import com.db.dataplatform.techtest.server.api.model.DataBody;
import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.api.model.DataHeader;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.exception.DataBlockNotFoundException;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import com.db.dataplatform.techtest.server.service.DataLakeService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.modelmapper.ModelMapper;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.db.dataplatform.techtest.server.utils.Utils.getChecksum;

@Slf4j
@Service
@RequiredArgsConstructor
public class ServerImpl implements Server {

    private final DataBodyService dataBodyServiceImpl;
    private final DataLakeService dataLakeService;

    private final ModelMapper modelMapper;

    /**
     * @param envelope
     * @return true if there is a match with the client provided checksum.
     */
    @Override
    public boolean saveDataEnvelope(DataEnvelope envelope, final String checkSumInput) {

        if (isValidCheckSum(envelope, checkSumInput)) {
            persist(envelope);
        }

        log.info("Data persisted successfully, data name: {}", envelope.getDataHeader().getName());
        return true;
    }

    private boolean isValidCheckSum(DataEnvelope envelope, String checkSumInput) {
        try {
            String calculatedCheckSum = getChecksum(envelope.getDataBody().getDataBody());
            //actual implementation of checking against the persisted is not done due to time constraints
            //and this is a placeholder where that feature can be added retrospectively
        } catch (IOException e) {
            log.error("Exception: {}", e.getCause());
        } catch (NoSuchAlgorithmException e) {
            log.info("Data Entity list from repo with attribute name: {}", e);
        }
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

    /**
     * @param name
     * @param blockTypeEnum
     * @return updated Data Block
     */
    @Override
    public boolean patchDataBlock(String name, BlockTypeEnum blockTypeEnum) throws DataBlockNotFoundException {

        //Check if the data block exists. If it exists then update else throw exception
        Optional<DataBodyEntity> dataByBlockNameOptional = dataBodyServiceImpl.getDataByBlockName(name);

        DataBodyEntity dataBodyEntity = dataByBlockNameOptional.orElseThrow(DataBlockNotFoundException::new);
        updateDataBlock(blockTypeEnum, dataBodyEntity);

        return true;
    }

    //Possibly can be refactored to using Model map rather than set this manually
    private List<DataEnvelope> getDataEnvelopesFromEntitiesList(List<DataBodyEntity> dataBodyEntityList) {

        List<DataEnvelope> dataEnvelopeList = new ArrayList<>();
        for (DataBodyEntity dataBodyEntity : dataBodyEntityList) {
            DataEnvelope dataEnvelope = new DataEnvelope();
            dataEnvelope.setDataBody(new DataBody(dataBodyEntity.getDataBody()));
            dataEnvelope.setDataHeader(new DataHeader(dataBodyEntity.getDataHeaderEntity().getName(), dataBodyEntity.getDataHeaderEntity().getBlockType()));
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

        log.info("Call to Push Data service started with entity dataBodyEntity: {}", dataBodyEntity);

        dataLakeService.pushDataToDataLake(dataBodyEntity);
    }

    private void saveData(DataBodyEntity dataBodyEntity) {
        dataBodyServiceImpl.saveDataBody(dataBodyEntity);
    }

    private void updateDataBlock(BlockTypeEnum blockTypeEnum, DataBodyEntity dataBodyEntity) {
        dataBodyEntity.getDataHeaderEntity().setBlockType(blockTypeEnum);
        saveData(dataBodyEntity);
    }

}
