package com.db.dataplatform.techtest.service;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.exception.DataBlockNotFoundException;
import com.db.dataplatform.techtest.server.mapper.ServerMapperConfiguration;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import com.db.dataplatform.techtest.server.persistence.model.DataBodyEntity;
import com.db.dataplatform.techtest.server.persistence.model.DataHeaderEntity;
import com.db.dataplatform.techtest.server.service.DataBodyService;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.component.impl.ServerImpl;
import com.db.dataplatform.techtest.server.service.DataLakeService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.modelmapper.ModelMapper;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.db.dataplatform.techtest.TechTestApplication.HEADER_NAME;
import static com.db.dataplatform.techtest.TestDataHelper.createTestDataEnvelopeApiObject;
import static org.assertj.core.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@RunWith(MockitoJUnitRunner.class)
public class ServerServiceTests {

    @Mock
    private DataBodyService dataBodyServiceImplMock;

    @Mock
    private DataLakeService dataLakeServiceMock;

    private ModelMapper modelMapper;

    private DataBodyEntity expectedDataBodyEntity;
    private DataEnvelope testDataEnvelope;

    private Server server;

    @Before
    public void setup() {
        ServerMapperConfiguration serverMapperConfiguration = new ServerMapperConfiguration();
        modelMapper = serverMapperConfiguration.createModelMapperBean();

        testDataEnvelope = createTestDataEnvelopeApiObject();
        expectedDataBodyEntity = modelMapper.map(testDataEnvelope.getDataBody(), DataBodyEntity.class);
        expectedDataBodyEntity.setDataHeaderEntity(modelMapper.map(testDataEnvelope.getDataHeader(), DataHeaderEntity.class));

        server = new ServerImpl(dataBodyServiceImplMock, dataLakeServiceMock, modelMapper);
    }

    @Test
    public void shouldSaveDataEnvelopeAsExpected() throws NoSuchAlgorithmException, IOException {
        boolean success = server.saveDataEnvelope(testDataEnvelope, "");

        assertThat(success).isTrue();
        verify(dataBodyServiceImplMock, times(1)).saveDataBody(any());
    }

    @Test
    public void shouldPatchDataBlockAsExpected() throws DataBlockNotFoundException {
        //Given
        when(dataBodyServiceImplMock.getDataByBlockName(anyString())).thenReturn(Optional.of(expectedDataBodyEntity));
        doNothing().when(dataBodyServiceImplMock).saveDataBody(any());

        //When
        boolean success = server.patchDataBlock(HEADER_NAME,BlockTypeEnum.BLOCKTYPEB);

        //Then
        assertThat(success).isTrue();
        verify(dataBodyServiceImplMock, times(1)).saveDataBody(eq(expectedDataBodyEntity));
    }

    @Test()
    public void shouldPatchDataBlockThrowExceptionWhenDataBlockNotFound() throws DataBlockNotFoundException {
        //Given
        when(dataBodyServiceImplMock.getDataByBlockName(anyString())).thenReturn(Optional.empty());

        //When and THen
        assertThatThrownBy(() -> server.patchDataBlock(HEADER_NAME,BlockTypeEnum.BLOCKTYPEB))
                .isInstanceOf(DataBlockNotFoundException.class);

    }

    @Test
    public void shouldGetDataEnvelopeListByBlockTypeExpected() {

        List<DataBodyEntity> dataBodyEntities = new ArrayList<>();
        dataBodyEntities.add(expectedDataBodyEntity);
        //Given
        when(dataBodyServiceImplMock.getDataByBlockType(BlockTypeEnum.BLOCKTYPEA)).thenReturn(dataBodyEntities);

        //When
        List<DataEnvelope> dataEnvelopeList = server.getDataEnvelopeListByBlockType(BlockTypeEnum.BLOCKTYPEA);

        //Then
        assertThat(dataEnvelopeList.size()).isEqualTo(1);
        verify(dataBodyServiceImplMock, times(1)).getDataByBlockType(eq(BlockTypeEnum.BLOCKTYPEA));
    }
}
