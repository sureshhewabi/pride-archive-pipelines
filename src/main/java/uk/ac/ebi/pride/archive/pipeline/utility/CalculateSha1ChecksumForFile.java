package uk.ac.ebi.pride.archive.pipeline.utility;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.security.DigestInputStream;
import java.security.MessageDigest;

public class CalculateSha1ChecksumForFile {

    private static final Integer BUFFER_SIZE = 2048;

    public static String calculateSha1Checksum(String filepath) throws IOException {
        byte[] bytesRead = new byte[BUFFER_SIZE];
        final MessageDigest inputStreamMessageDigest = Hash.getSha1();
        final DigestInputStream digestInputStream = new DigestInputStream(new FileInputStream(new File(filepath)), inputStreamMessageDigest);
        while (digestInputStream.read(bytesRead) != -1) ;
        return Hash.normalize(inputStreamMessageDigest);
    }
}
