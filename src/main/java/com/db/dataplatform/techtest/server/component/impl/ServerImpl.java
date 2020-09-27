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
import static java.util.Objects.nonNull;

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

        final boolean isValidCheckSum = isValidCheckSum(envelope, checkSumInput);
        log.info("CheckSum valid ? ={}", isValidCheckSum);

        if (isValidCheckSum) {
            persist(envelope, checkSumInput);
        }

        log.info("Data persisted successfully, data name: {}", envelope.getDataHeader().getName());
        return isValidCheckSum;
    }

    private boolean isValidCheckSum(DataEnvelope envelope, String checkSumInput) {

        String calculatedCheckSum = calculateCheckSum(envelope);

        log.info("calculated Checkum ={}", calculatedCheckSum);

        log.info("Checking if Valid check sum for checksum input ={}, calculateCheckSum={}", checkSumInput, calculatedCheckSum);

        return nonNull(calculatedCheckSum) && checkSumInput.equals(calculatedCheckSum);
    }

    private String calculateCheckSum(DataEnvelope envelope) {

        log.info("calculating checkSum");

        String calculatedCheckSum = null;
        try {
            calculatedCheckSum = getChecksum(envelope.getDataBody().getDataBody());
        } catch (IOException e) {
            log.error("Exception: {}", e.getCause());
        } catch (NoSuchAlgorithmException e) {
            log.info("Data Entity list from repo with attribute name: {}", e);
        }
        return calculatedCheckSum;
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

    private void persist(DataEnvelope envelope, String checkSumInput) {
        log.info("Persisting data with attribute name: {}", envelope.getDataHeader().getName());
        DataHeaderEntity dataHeaderEntity = modelMapper.map(envelope.getDataHeader(), DataHeaderEntity.class);
        dataHeaderEntity.setCheckSum(checkSumInput);

        DataBodyEntity dataBodyEntity = modelMapper.map(envelope.getDataBody(), DataBodyEntity.class);
        dataBodyEntity.setDataHeaderEntity(dataHeaderEntity);

        saveData(dataBodyEntity);

        log.info("Call to Push Data Lake started with entity dataBodyEntity: {}", dataBodyEntity);

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
