package uk.ac.ebi.pride.archive.pipeline.utility;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.paranamer.ParanamerModule;
import uk.ac.ebi.pride.mongodb.molecules.model.peptide.PrideMongoPeptideEvidence;
import uk.ac.ebi.pride.mongodb.molecules.model.protein.PrideMongoProteinEvidence;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class BackupUtil {

    private static final ObjectMapper objectMapper;
    private static final String JSON_EXT = ".json";

    static {
        objectMapper = new ObjectMapper();
        objectMapper.registerModule(new ParanamerModule());
    }

    public static void write(Object obj, BufferedWriter bw) throws Exception {
        String s = objectMapper.writeValueAsString(obj);
        bw.write(s);
        bw.newLine();
    }

    public static String getPrideMongoProteinEvidenceFile(String backupPath, String projectAccession, String assayAccession) {
        if (!backupPath.endsWith(File.separator)) {
            backupPath = backupPath + File.separator;
        }
        return backupPath + projectAccession + File.separator + projectAccession + "_" + assayAccession +
                "_" + PrideMongoProteinEvidence.class.getSimpleName() + JSON_EXT;
    }

    public static String getPrideMongoPeptideEvidenceFile(String backupPath, String projectAccession, String assayAccession) {
        if (!backupPath.endsWith(File.separator)) {
            backupPath = backupPath + File.separator;
        }
        return backupPath + projectAccession + File.separator + projectAccession + "_" + assayAccession +
                "_" + PrideMongoPeptideEvidence.class.getSimpleName() + JSON_EXT;
    }

    public static List<PrideMongoProteinEvidence> getPrideMongoProteinEvidenceFromBackup(String backupPath, String projectAccession, String assayAccession) throws IOException {
        List<PrideMongoProteinEvidence> prideMongoProteinEvidences = new ArrayList<>();
        BufferedReader reader;
            reader = new BufferedReader(new FileReader(getPrideMongoProteinEvidenceFile(backupPath, projectAccession, assayAccession)));
            String line = reader.readLine();
            while (line != null) {
                prideMongoProteinEvidences.add(objectMapper.readValue(line, PrideMongoProteinEvidence.class));
                line = reader.readLine();
            }
            reader.close();

        return prideMongoProteinEvidences;
    }

    public static List<PrideMongoPeptideEvidence> getPrideMongoPeptideEvidenceFromBackup(String backupPath, String projectAccession, String assayAccession) throws IOException {
        List<PrideMongoPeptideEvidence> prideMongoPeptideEvidences = new ArrayList<>();
        BufferedReader reader;
        reader = new BufferedReader(new FileReader(getPrideMongoPeptideEvidenceFile(backupPath, projectAccession, assayAccession)));
        String line = reader.readLine();
        while (line != null) {
            prideMongoPeptideEvidences.add(objectMapper.readValue(line, PrideMongoPeptideEvidence.class));
            line = reader.readLine();
        }
        reader.close();

        return prideMongoPeptideEvidences;
    }
}
