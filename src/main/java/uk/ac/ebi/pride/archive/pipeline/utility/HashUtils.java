package uk.ac.ebi.pride.archive.pipeline.utility;

import org.apache.commons.codec.digest.DigestUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class HashUtils {

    public static String getSha1Checksum(File file) throws IOException {
        return DigestUtils.sha1Hex(new FileInputStream(file));
    }

    public static String getSha256Checksum(String str) {
        return DigestUtils.sha256Hex(str);
    }
}