package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.repo.files.PrideFileMongoRepository;
import uk.ac.ebi.pride.mongodb.archive.service.files.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class})
public class MongoChecksumJob extends AbstractArchiveJob {

    @Autowired
    PrideFileMongoService prideFileMongoService;

    @Autowired
    PrideProjectMongoService projectMongoService;

    @Autowired
    PrideFileMongoRepository mongoFileRepository;

    @Value("${path:#{null}}")
    private String path;

    private Map<String, Long> taskTimeMap = new HashMap<>();

    @Bean
    public Job mongoChecksumJobBean() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_MONGO_CHECKSUM.getName())
                .start(populateMongoChecksumStep())
                .next(mongoChecksumPrintTraceStep())
                .build();
    }

    @Bean
    public Step mongoChecksumPrintTraceStep() {
        return stepBuilderFactory
                .get("mongoChecksumPrintTraceStep")
                .tasklet((stepContribution, chunkContext) -> {
                    taskTimeMap.forEach((key, value) -> log.info("Task: " + key + " Time: " + value));
                    return RepeatStatus.FINISHED;
                }).build();
    }

    @Bean
    public Step populateMongoChecksumStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGO_CHECKSUM.name())
                .tasklet((stepContribution, chunkContext) -> {

                    long initTime = System.currentTimeMillis();

                    Map<String, String> checksumMap = new HashMap<>();
                    Files.newDirectoryStream(Paths.get(path), f -> f.toString().endsWith(".csv"))
                            .forEach(f -> {
                                String prjAccession = f.getFileName().toString().split("-")[2].replaceAll(".csv", "");
                                try {
//                                    System.out.println(f.getFileName());
                                    Files.lines(f).filter(l -> !l.isEmpty()).forEach(l -> {
                                        int spaceIndex = l.indexOf(" ");
                                        String checksum = l.substring(0, spaceIndex).trim();
                                        String filePath = l.substring(spaceIndex).trim();
                                        String fName = filePath.substring(filePath.lastIndexOf("/") + 1);
//                                        System.out.println(fName);
//                                        System.out.println(checksum);
                                        checksumMap.put(prjAccession + "-" + fName, checksum);
                                    });
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            });

//                    System.out.println(map.size());

                    List<String> allProjectAccessions = projectMongoService.getAllProjectAccessions();
                    allProjectAccessions.stream().forEach(p -> {
                        List<MongoPrideFile> mongoFiles = prideFileMongoService.findFilesByProjectAccession(p);
//                        System.out.println(mongoFiles.size());
                        mongoFiles.stream().forEach(mongoPrideFile -> {
//                            System.out.println(i);
                            String prjAccession = mongoPrideFile.getProjectAccessions().stream().findFirst().get();
                            String key = prjAccession + "-" + mongoPrideFile.getFileName();
                            String cs = checksumMap.get(key);
                            if (cs != null && mongoPrideFile.getChecksum() == null) {
                                mongoPrideFile.setChecksum(cs);
                                mongoFileRepository.save(mongoPrideFile);
                            }
                        });
                    });

                    taskTimeMap.put("populateMongoChecksumStep", System.currentTimeMillis() - initTime);

                    return RepeatStatus.FINISHED;
                }).build();
    }

}