package com.db.dataplatform.techtest.server.utils;

import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import javax.xml.bind.DatatypeConverter;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;

public class Utils {

    public static final String MD_5 = "MD5";

    public static HttpHeaders getHttpHeaders() {
        HttpHeaders headers = new HttpHeaders();
        headers.setAccept(Collections.singletonList(MediaType.APPLICATION_JSON));
        return headers;
    }

    public static String getChecksum(Serializable object) throws IOException, NoSuchAlgorithmException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(); ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(object);
            MessageDigest md = MessageDigest.getInstance(MD_5);
            byte[] digest = md.digest(baos.toByteArray());
            return DatatypeConverter.printHexBinary(digest);
        }
    }
}
