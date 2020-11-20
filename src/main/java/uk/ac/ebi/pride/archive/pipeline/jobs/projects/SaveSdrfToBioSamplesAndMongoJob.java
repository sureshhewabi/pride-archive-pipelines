package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import com.univocity.parsers.common.record.Record;
import com.univocity.parsers.tsv.TsvParser;
import com.univocity.parsers.tsv.TsvParserSettings;
import com.univocity.parsers.tsv.TsvWriter;
import com.univocity.parsers.tsv.TsvWriterSettings;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.hateoas.Resource;
import uk.ac.ebi.biosamples.client.BioSamplesClient;
import uk.ac.ebi.biosamples.model.Attribute;
import uk.ac.ebi.biosamples.model.ExternalReference;
import uk.ac.ebi.biosamples.model.Relationship;
import uk.ac.ebi.biosamples.model.Sample;
import uk.ac.ebi.biosamples.model.SubmittedViaType;
import uk.ac.ebi.pride.archive.dataprovider.common.Tuple;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.HashUtils;
import uk.ac.ebi.pride.archive.pipeline.utility.PrideFilePathUtility;
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.model.sdrf.MongoPrideSdrf;
import uk.ac.ebi.pride.mongodb.archive.service.files.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.sdrf.PrideSdrfMongoService;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static uk.ac.ebi.pride.archive.pipeline.utility.PrideFilePathUtility.SUBMITTED;

/*
    This Job is get biosample id for samples and save to mongo - post publication
 */
@Configuration
@Slf4j
@EnableBatchProcessing
public class SaveSdrfToBioSamplesAndMongoJob extends AbstractArchiveJob {

    public static final String SAVE_SDRF_TO_BIO_SAMPLES_AND_MONGO_JOB = "sdrfSaveToBioSamplesAndMongoJob";
    public static final String READ_TSV_STEP = "readTsvStep";
    public static final String SAVE_TO_BIO_SAMPLES_AND_MONGO_STEP = "saveToBioSamplesAndMongoStep";
    public static final String PRIDE_DOMAIN = "self.pride";
    public static final String SOURCE_NAME = "source name";
    public static final String PRIDE_ARCHIVE_PROJECT_URL = "https://www.ebi.ac.uk/pride/archive/projects/";
    public static final String SAMPLE_CHECKSUM = "sampleChecksum";
    public static final String SAMPLE_ACCESSION = "sampleAccession";
    private static final String SYNC_TO_FTP = "syncToFtp";
    private static final String SYNC_SCRIPT = "./syncProjectLDC.sh";
    public static final String BECOME_PRIDE_ADM_CP = "become pride_adm cp ";
    public static final String TSV = ".tsv";
    public static final String SDRF = "sdrf";
    public static final String SYNC_SDRF_FILES_TO_MONGO = "syncSdrfFilesToMongo";
    public static final String TMP = ".tmp";
    public static final String MAKE_SAMPLE_ACCESSIONS_PRIVATE_TXT = "makeSampleAccessionsPrivate.txt";

    @Value("${pride.archive.data.path}")
    private String prideRepoRootPath;

    @Value(("${ldc.staging.base.dir}"))
    private String ldcStagingBaseFolder;

    @Value(("${accession:null}"))
    private String accession;

    @Autowired
    private BioSamplesClient bioSamplesClient;

    @Autowired
    private PrideSdrfMongoService prideSdrfMongoService;

    private List<MongoPrideSdrf> samplesToSave = new ArrayList<>();

    @Autowired
    private PrideProjectMongoService prideProjectMongoService;

    @Autowired
    private PrideFileMongoService prideFileMongoService;

    private Map<String, Tuple<String, List<Record>>> sdrfContentsToProcess = new HashMap<>();

    private String submittedFilesPath;

    @Bean
    public Job sdrfSaveToBioSamplesAndMongoJob() {
        return jobBuilderFactory
                .get(SAVE_SDRF_TO_BIO_SAMPLES_AND_MONGO_JOB)
                .start(readTsvStep())
                .next(sdrfSaveToBioSamplesAndMongoStep())
                .next(syncSdrfFilesToMongo())
                .next(syncToFtp())
                .build();
    }


    private Step readTsvStep() {
        return stepBuilderFactory
                .get(READ_TSV_STEP)
                .tasklet(readTsvTasklet()).build();
    }

    private Step sdrfSaveToBioSamplesAndMongoStep() {
        return stepBuilderFactory
                .get(SAVE_TO_BIO_SAMPLES_AND_MONGO_STEP)
                .tasklet(saveToBioSamplesAndMongoTasklet()).build();
    }


    private Step syncSdrfFilesToMongo() {
        return stepBuilderFactory.get(SYNC_SDRF_FILES_TO_MONGO)
                .tasklet((stepContribution, chunkContext) -> {
                    doSdrfFileSync(sdrfContentsToProcess);
                            return RepeatStatus.FINISHED;
                        }
                ).build();
    }

    private Step syncToFtp() {
        return stepBuilderFactory.get(SYNC_TO_FTP)
                .tasklet(syncToFtpTasklet()).build();
    }

    private void doSdrfFileSync(Map<String, Tuple<String, List<Record>>> files) {
        try {
            List<MongoPrideFile> mongoFiles =
                    prideFileMongoService.findFilesByProjectAccession(accession);
            for (MongoPrideFile mongoPrideFile : mongoFiles) {
                files.entrySet().stream().forEach(map -> {
                            try {
                                String processedSdrfFileName = map.getValue().getKey();
                                if (processedSdrfFileName.contains(mongoPrideFile.getFileName())) {
                                    File processedSdrfFile = new File(processedSdrfFileName);
                                    mongoPrideFile.setChecksum(HashUtils.getSha1Checksum(processedSdrfFile));
                                    mongoPrideFile.setFileSizeBytes(Files.size(processedSdrfFile.toPath()));
                                    prideFileMongoService.save(mongoPrideFile);
                                    log.info("File " + mongoPrideFile.getFileName() + " is synced to mongo with new checksum");
                                }
                            } catch (IOException e) {
                                log.error(e.getMessage(), e);
                                throw new RuntimeException("Error in calculating checksum/Parsing file");
                            }
                        }
                );
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            throw new IllegalStateException(e);
        }
    }

    private Tasklet readTsvTasklet() {
        return (stepContribution, chunkContext) -> {
            TsvParserSettings tsvParserSettings = new TsvParserSettings();
            tsvParserSettings.setNullValue("not available");
            submittedFilesPath = PrideFilePathUtility
                    .getSubmittedFilesPath(prideProjectMongoService.findByAccession(accession).get(), prideRepoRootPath);
            Map<String, Tuple<String, List<Record>>> sdrfContentsToProcess = new HashMap<>();
            Files.walk(Paths.get(submittedFilesPath), Integer.MAX_VALUE).skip(1).forEach(file -> {
                String fileName = file.getFileName().toString();
                if (fileName.endsWith(TSV) && fileName.contains(SDRF)) {
                    try {
                        File fileObject = file.toFile();
                        String fileChecksum = HashUtils.getSha1Checksum(fileObject);
                        if (checkFilesAlreadySaved(fileChecksum)) {
                            return;
                        }
                        TsvParser tsvParser = new TsvParser(tsvParserSettings);
                        List<Record> records = tsvParser.parseAllRecords(fileObject);
                        if (Arrays.asList(records.get(0).getMetaData().headers()).contains(SAMPLE_ACCESSION)) {
                            return;
                        }
                        sdrfContentsToProcess.put(fileChecksum, new Tuple(submittedFilesPath + fileName,
                                records));
                    } catch (Exception e) {
                        throw new RuntimeException("Error in calculating checksum/Parsing file  " + fileName);
                    }
                }
            });
            return RepeatStatus.FINISHED;
        };
    }


    /*
     This method is to check whether the project is being republished with same sdrf.
     */
    private boolean checkFilesAlreadySaved(String fileChecksum) {
        Set<String> fileChecksums = prideSdrfMongoService.getUniqueFileChecksumsOfProject(accession);
        if (fileChecksums.contains(fileChecksum)) {
            return true;
        }
        return false;
    }

    private Tasklet saveToBioSamplesAndMongoTasklet() {
        return (stepContribution, chunkContext) -> {
            Map<String, String> sampleChecksumAccession = getSampleChecksumAccession();
            Set<String> sampleAccessionsToMakePrivate = new HashSet<>(sampleChecksumAccession.values());
            for (Map.Entry<String, Tuple<String, List<Record>>> fileCheckSumToSdrfContent : sdrfContentsToProcess.entrySet()) {
                String fileChecksum = fileCheckSumToSdrfContent.getKey();
                List<Record> sdrfRecords = fileCheckSumToSdrfContent.getValue().getValue();
                String[] headers = sdrfRecords.get(0).getMetaData().headers();
                String submittedSdrfFilePath = fileCheckSumToSdrfContent.getValue().getKey();
                TsvWriter tsvWriter = getTsvWriter(new File(submittedSdrfFilePath + TMP), headers);
                sdrfRecords.remove(0); // remove header
                for (Record sdrfRecord : sdrfRecords) {
                    String sampleName = accession + "-" + sdrfRecord.getString(SOURCE_NAME);
                    Sample sample = Sample.build(sampleName,
                            null, PRIDE_DOMAIN, Instant.now(), null,
                            getAttributes(headers, sdrfRecord), getRelationShip(headers, sdrfRecord),
                            getExternalReferences(headers, sdrfRecord, accession), SubmittedViaType.JSON_API);

                    // This is to check whether the sample already saved to biosamples
                    String sampleChecksum = HashUtils.getSha256Checksum(sampleName + sample.getAttributes().toString());
                    String sampleAccession = "";
                    if (!sampleChecksumAccession.containsKey(sampleChecksum)) {
                        Resource<Sample> sampleResource = bioSamplesClient.persistSampleResource(sample);
                        sampleAccession = sampleResource.getContent().getAccession();
                        sampleChecksumAccession.put(sampleChecksum, sampleAccession);
                    } else {
                        sampleAccession = sampleChecksumAccession.get(sampleChecksum);
                    }
                    sampleAccessionsToMakePrivate.remove(sampleAccession);
                    Map<String, String> sampleToSave = createSample(headers, sdrfRecord, sampleAccession, sampleChecksum);
                    sampleToSave.put(SOURCE_NAME, sampleName);
                    saveSamplesToMongo(accession, fileChecksum, sampleToSave);
                    saveRowToFile(tsvWriter, sdrfRecord, sampleName, sampleChecksum, sampleAccession, headers);
                }
                tsvWriter.close();
                copyToStaging(submittedSdrfFilePath);
            }
            Files.write(Paths.get(new File(submittedFilesPath + MAKE_SAMPLE_ACCESSIONS_PRIVATE_TXT).getPath()),
                    sampleAccessionsToMakePrivate,
                    Charset.defaultCharset());
            if (samplesToSave.size() > 0) {
                prideSdrfMongoService.saveSdrfList(samplesToSave);
            }
            return RepeatStatus.FINISHED;
        };
    }

    private void copyToStaging(String submittedSdrfFilePath) {
        try {
            Process renameOriginalSdrf = Runtime.getRuntime().exec("cp " + submittedSdrfFilePath +
                    " " + submittedSdrfFilePath + ".submitted");
            Process renameTmpToOriginalSdrf = Runtime.getRuntime().exec("cp " + submittedSdrfFilePath + TMP + " " +
                    submittedSdrfFilePath);
            Process cpSdrf = Runtime.getRuntime().exec(BECOME_PRIDE_ADM_CP + submittedSdrfFilePath
                    + " " + submittedSdrfFilePath
                    .replace(prideRepoRootPath, ldcStagingBaseFolder)
                    .replace(SUBMITTED + File.separator, ""));
            try {
                renameOriginalSdrf.waitFor();
                renameTmpToOriginalSdrf.waitFor();
                log.info("Copying Sdrf file to staging");
                cpSdrf.waitFor();
            } catch (InterruptedException e) {
                log.error("Error copying to staging area" + "\n" + e.getMessage());
            }
        } catch (IOException e) {
            String errorMessage = "Error copying to staging area";
            log.error(errorMessage + "\n" + e.getMessage());
            throw new RuntimeException(errorMessage);
        }
    }


    private void saveRowToFile(TsvWriter tsvWriter, Record sdrfRecord,
                               String sampleName, String sampleChecksum,
                               String sampleAccession, String[] headers) {
        for (String header : headers) {
            if (header.toLowerCase().equals(SOURCE_NAME)) {
                tsvWriter.addValue(SOURCE_NAME, sampleName);
            } else {
                tsvWriter.addValue(header, sdrfRecord.getString(header));
            }
        }
        tsvWriter.addValue(SAMPLE_CHECKSUM, sampleChecksum);
        tsvWriter.addValue(SAMPLE_ACCESSION, sampleAccession);
        tsvWriter.writeValuesToRow();
    }

    private TsvWriter getTsvWriter(File file, String[] headers) {
        List<String> headersList = new ArrayList<>();
        headersList.addAll(Arrays.asList(headers));
        headersList.add(SAMPLE_CHECKSUM);
        headersList.add(SAMPLE_ACCESSION);
        log.info("Creating output File " + file.getAbsolutePath());
        TsvWriter tsvWriter = new TsvWriter(file, new TsvWriterSettings());
        tsvWriter.writeHeaders(headersList);
        return tsvWriter;
    }

    private Map<String, String> getSampleChecksumAccession() {
        List<MongoPrideSdrf> mongoPrideSdrfList = prideSdrfMongoService.findByProjectAccession(accession);
        Map<String, String> sampleChecksumAccession = new HashMap<>();
        if (mongoPrideSdrfList != null) {
            mongoPrideSdrfList.stream().forEach(
                    mongoPrideSdrf -> {
                        sampleChecksumAccession.put(mongoPrideSdrf.getSample().get(SAMPLE_CHECKSUM),
                                mongoPrideSdrf.getSample().get(SAMPLE_ACCESSION));
                    }
            );
        }
        return sampleChecksumAccession;
    }

    private Set<ExternalReference> getExternalReferences(String[] headers, Record sdrfObject, String projectAccession) {
        Set<ExternalReference> externalReferences = new HashSet<>();
        externalReferences.add(ExternalReference.build(PRIDE_ARCHIVE_PROJECT_URL + projectAccession));
        return null;
    }

    private Set<Relationship> getRelationShip(String[] headers, Record sdrfObject) {
        return null;
    }

    private Set<Attribute> getAttributes(String[] headers, Record sdrfObject) {
        Set<Attribute> attributes = new HashSet<>();
        attributes.add(Attribute.build("project", "PRIDE"));
        for (String columnName : headers) {
            if (columnName.toLowerCase().startsWith("characteristics")) {
                attributes.add(Attribute.build(columnName.substring(16, columnName.length() - 1), sdrfObject.getString(columnName)));
            }
        }
        return attributes;
    }


    private Map<String, String> createSample(String headers[], Record sdrfObject, String sampleAccession, String sampleChecksum) {
        Set<String> columnNames = new HashSet<>();
        int count = 1;
        Map<String, String> sample = new HashMap<>();
        for (int i = 0; i < headers.length; i++) {
            String columnName = headers[i].toLowerCase();
            if (!columnNames.add(columnName)) {
                sample.put(columnName + "_" + count++, sdrfObject.getString(i));
            } else {
                sample.put(columnName, sdrfObject.getString(i));
            }
        }
        sample.put(SAMPLE_ACCESSION, sampleAccession);
        sample.put(SAMPLE_CHECKSUM, sampleChecksum);
        return sample;
    }

    private void saveSamplesToMongo(String accession, String fileChecksum,
                                    Map<String, String> sample) {
        samplesToSave.add(MongoPrideSdrf.builder()
                .projectAccession(accession)
                .filechecksum(fileChecksum)
                .sample(sample)
                .build());
        if (samplesToSave.size() >= 1000) {
            prideSdrfMongoService.saveSdrfList(samplesToSave);
            samplesToSave.clear();
        }
    }

    private Tasklet syncToFtpTasklet() {
        return (stepContribution, chunkContext) -> {
            log.info("Executing sync shell script: " + SYNC_SCRIPT);
            Process p = new ProcessBuilder(SYNC_SCRIPT).start();
            p.waitFor();
        /* correct 'snap-release pride' output behaviour:
            [INFO] Running snap-release for pride
            [SUCCESS] Snap-release finished for project pride
           anything else, or no output, is an error
        */
            InputStream inputStream = p.getInputStream();
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            String line;
            boolean syncSuccess = false;
            while ((line = bufferedReader.readLine()) != null) {
                log.info(line);
                if (line.contains("SUCCESS")) {
                    syncSuccess = true;
                }
            }
            bufferedReader.close();
            inputStream.close();
            if (!syncSuccess) {
                inputStream = p.getErrorStream();
                String msg = "Failed to sync project with LDC";
                log.error(msg);
                bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                while ((line = bufferedReader.readLine()) != null) {
                    log.error(line);
                }
                throw new IllegalStateException(msg);
            }
            return RepeatStatus.FINISHED;
        };
    }
}
