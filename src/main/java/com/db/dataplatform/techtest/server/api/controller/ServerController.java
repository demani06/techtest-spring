package com.db.dataplatform.techtest.server.api.controller;

import com.db.dataplatform.techtest.server.api.model.DataEnvelope;
import com.db.dataplatform.techtest.server.component.Server;
import com.db.dataplatform.techtest.server.exception.DataBlockNotFoundException;
import com.db.dataplatform.techtest.server.persistence.BlockTypeEnum;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

@Slf4j
@Controller
@RequestMapping("/dataserver")
@RequiredArgsConstructor
@Validated
public class ServerController {

    private final Server server;

    @PostMapping(value = "/pushdata", consumes = MediaType.APPLICATION_JSON_VALUE, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<Boolean> pushData(@Valid @RequestBody DataEnvelope dataEnvelope,
                                            @RequestHeader(required = false) String checkSum)
            throws IOException, NoSuchAlgorithmException {

        log.info("Data envelope received: {}", dataEnvelope.getDataHeader().getName());
        boolean checkSumOutput = server.saveDataEnvelope(dataEnvelope, checkSum);

        log.info("Push Data Envelope checkSumOutput : {}", checkSumOutput);
        return ResponseEntity.ok(checkSumOutput);
    }

    @GetMapping(value = "/data/{blockType}")
    public ResponseEntity<List<DataEnvelope>> getPersistedBlockByBlockType
            (@PathVariable(value="blockType") BlockTypeEnum blockTypeEnum) {

        log.info("Getting all Data envelope by block type: {}", blockTypeEnum);

        List<DataEnvelope> dataEnvelopeList = server.getDataEnvelopeListByBlockType(blockTypeEnum);

        log.info("Got all Data envelopes by block type: {} and list : {}", blockTypeEnum, dataEnvelopeList);

        return ResponseEntity.ok(dataEnvelopeList);
    }

    /*
    * Validations added to name as part of the requirement - not to be blank and max size of 20
    * */
    @PatchMapping(value = "/update/{name}/{newBlockType}")
    public ResponseEntity<Boolean> patchPersistedBlockByBlockType(
            @NotBlank @Size(max = 20) @PathVariable(value="name") String name,
            @PathVariable(value="newBlockType") String newBlockType)
            throws DataBlockNotFoundException {

        log.info("Trying to patch Data block with name : {} by block type: {}", name, newBlockType);

        final boolean patchDataBlock = server.patchDataBlock(name, BlockTypeEnum.valueOf(newBlockType));

        log.info("Patched Data block with name : {} by block type: {}", name, newBlockType);

        return ResponseEntity.ok(patchDataBlock);
    }

}
