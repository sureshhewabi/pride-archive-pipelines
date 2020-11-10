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
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.model.sdrf.MongoPrideSdrf;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.sdrf.PrideSdrfMongoService;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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

    @Value("${pride.archive.data.path}")
    private String prideRepoRootPath;

    @Value("${githubFolder:#{null}}")
    private String folderPath;

    @Autowired
    private BioSamplesClient bioSamplesClient;

    @Autowired
    private PrideSdrfMongoService prideSdrfMongoService;

    private List<MongoPrideSdrf> samplesToSave = new ArrayList<>();

    @Autowired
    private PrideProjectMongoService prideProjectMongoService;

    private Map<String, Map<String, Tuple<String, List<Record>>>> accessionToSdrfContents = new HashMap<>();

    @Bean
    public Job sdrfSaveToBioSamplesAndMongoJob() {
        return jobBuilderFactory
                .get(SAVE_SDRF_TO_BIO_SAMPLES_AND_MONGO_JOB)
                .start(readTsvStep())
                .next(sdrfSaveToBioSamplesAndMongoStep())
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

    private Tasklet readTsvTasklet() {
        return (stepContribution, chunkContext) -> {
            Map<String, Tuple<String, List<Record>>> sdrfContentsToProcess = new HashMap<>();
            TsvParserSettings tsvParserSettings = new TsvParserSettings();
            tsvParserSettings.setNullValue("not available");
            Files.walk(Paths.get(folderPath), Integer.MAX_VALUE).skip(1).forEach(file -> {
                String fileName = file.getFileName().toString();
                if (fileName.endsWith(".tsv")) {
                    String accession = file.getParent().getFileName().toString();
                    try {
                        String fileChecksum = HashUtils.getSha1Checksum(file.toFile());
                        if (!checkFilesAlreadySaved(fileChecksum, accession)) {
                            TsvParser tsvParser = new TsvParser(tsvParserSettings);
                            sdrfContentsToProcess.put(fileChecksum, new Tuple(fileName,
                                    tsvParser.parseAllRecords(file.toFile())));
                        }
                    } catch (Exception e) {
                        throw new RuntimeException("Error in calculating checksum");
                    }
                    accessionToSdrfContents.put(accession, sdrfContentsToProcess);
                }
            });
            return RepeatStatus.FINISHED;
        };
    }


    /*
     This method is to check whether the project is being republished with same sdrf.
     */
    private boolean checkFilesAlreadySaved(String fileChecksum, String projectAccession) {
        Set<String> fileChecksums = prideSdrfMongoService.getUniqueFileChecksumsOfProject(projectAccession);
        if (fileChecksums.contains(fileChecksum)) {
            return true;
        }
        return false;
    }

    private Tasklet saveToBioSamplesAndMongoTasklet() {
        return (stepContribution, chunkContext) -> {
            for (Map.Entry<String, Map<String, Tuple<String,List<Record>>>> accessionSdrfContent : accessionToSdrfContents.entrySet()) {
                String projectAccession = accessionSdrfContent.getKey();
                saveToBioSampleAndMongo(projectAccession, accessionSdrfContent.getValue());
            }
            return RepeatStatus.FINISHED;
        };
    }

    private void saveToBioSampleAndMongo(String accession, Map<String, Tuple<String,List<Record>>> fileCheckSumToSdrfContents) {
        Map<String, String> sampleChecksumAccession = getSampleChecksumAccession(accession);
        for (Map.Entry<String, Tuple<String,List<Record>>> fileCheckSumToSdrfContent : fileCheckSumToSdrfContents.entrySet()) {
            String fileChecksum = fileCheckSumToSdrfContent.getKey();
            List<Record> sdrfRecords = fileCheckSumToSdrfContent.getValue().getValue();
            String[] headers = sdrfRecords.get(0).getMetaData().headers();
            TsvWriter tsvWriter = getTsvWriter(accession,
                    fileCheckSumToSdrfContent.getValue().getKey(), headers);
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
                Map<String, String> sampleToSave = createSample(headers, sdrfRecord, sampleAccession, sampleChecksum);
                sampleToSave.put(headers[0], sampleName);
                saveSamplesToMongo(accession, fileChecksum, sampleToSave);
                saveRowToFile(tsvWriter, sdrfRecord, sampleName, sampleChecksum, sampleAccession);
            }
            tsvWriter.close();
        }
        if (samplesToSave.size() > 0) {
            prideSdrfMongoService.saveSdrfList(samplesToSave);
        }
    }

    private void saveRowToFile(TsvWriter tsvWriter, Record sdrfRecord, String sampleName, String sampleChecksum, String sampleAccession) {
        tsvWriter.addValue(SOURCE_NAME, sampleName);
        List<String> row = new ArrayList<>();
        row.addAll(Arrays.asList(sdrfRecord.getValues()));
        row.remove(0); //remove original source name
        tsvWriter.addValues(row);
        tsvWriter.addValue(SAMPLE_CHECKSUM, sampleChecksum);
        tsvWriter.addValue(SAMPLE_ACCESSION, sampleAccession);
        tsvWriter.writeValuesToRow();
    }

    private TsvWriter getTsvWriter(String accession, String fileName, String[] headers) {
        List<String> headersList = new ArrayList<>();
        headersList.addAll(Arrays.asList(headers));
        headersList.add(SAMPLE_CHECKSUM);
        headersList.add(SAMPLE_ACCESSION);
        File outputFile = new File(getSdrfFilePath(accession) + fileName);
        log.info("Creating output File " + outputFile.getName());
        TsvWriter tsvWriter = new TsvWriter(outputFile, new TsvWriterSettings());
        tsvWriter.writeHeaders(headersList);
        return tsvWriter;
    }

    private String getSdrfFilePath(String accession) {
        Optional<MongoPrideProject> mongoPrideProject = prideProjectMongoService.findByAccession(accession);
        return PrideFilePathUtility.getSubmittedFilesPath(mongoPrideProject.get(), prideRepoRootPath);
    }

    private Map<String, String> getSampleChecksumAccession(String accession) {
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
            String columnName = headers[i];
            if (!columnNames.add(columnName)) {
                sample.put(columnName + "_" + count++, sdrfObject.getString(i));
            } else {
                sample.put(columnName, sdrfObject.getString(columnName));
            }
        }
        sample.put(SAMPLE_ACCESSION, sampleAccession);
        sample.put(SAMPLE_CHECKSUM, sampleChecksum);
        return sample;
    }

    private void saveSamplesToMongo(String accession, String fileChecksum,
                                    Map<String, String> sample) {
        String projectAccession = accession;
        samplesToSave.add(MongoPrideSdrf.builder()
                .projectAccession(projectAccession)
                .filechecksum(fileChecksum)
                .sample(sample)
                .build());
        if (samplesToSave.size() >= 1000) {
            prideSdrfMongoService.saveSdrfList(samplesToSave);
            samplesToSave.clear();
        }
    }
}
