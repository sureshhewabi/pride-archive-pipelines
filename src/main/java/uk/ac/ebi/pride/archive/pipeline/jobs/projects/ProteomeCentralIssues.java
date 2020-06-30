package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveOracleConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.px.PostMessage;
import uk.ac.ebi.pride.archive.px.Util;
import uk.ac.ebi.pride.archive.px.ValidateMessage;
import uk.ac.ebi.pride.archive.px.writer.MessageWriter;
import uk.ac.ebi.pride.archive.px.xml.XMLParams;
import uk.ac.ebi.pride.archive.repo.repos.file.ProjectFileRepository;
import uk.ac.ebi.pride.archive.repo.repos.project.Project;
import uk.ac.ebi.pride.archive.repo.repos.project.ProjectRepository;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveOracleConfig.class, DataSourceConfiguration.class})
public class ProteomeCentralIssues extends AbstractArchiveJob {

    @Autowired
    ProjectRepository oracleProjectRepository;

    @Value("${pride.proteomecentral-issues.path}")
    private String pcIssuesBasePath;

    @Value("${pride.archive.data.path}")
    private String prideDataPath;

    @Value("${px.partner.name}")
    private String pxPartnerName;

    @Value("${px.partner.pass}")
    private String pxPassword;

    private Map<String, Long> taskTimeMap = new HashMap<>();

    private String[] projectAccessions;
    private Path pxXmlsDir;
    private Path pcIssuesDir;
    private Path errorsDir;
    private XMLParams xmlParams;

    private static final String pxSchemaVersion = "1.4.0";
    private static final MessageWriter pxXmlMessageWriter = Util.getSchemaStrategy(pxSchemaVersion);

    @Bean
    @StepScope
    public Tasklet initPcIssuesJob(@Value("#{jobParameters['projects']}") String projects) {
        return (stepContribution, chunkContext) ->
        {
            if (projects != null) {
                this.projectAccessions = projects.split(",");
            }
            log.info(String.format("==================>>>>>>> initPcIssuesJob - Run the job for Project %s", Arrays.toString(projectAccessions)));

            DateFormat df = new SimpleDateFormat("yyyy-MM-dd-HH-mm-ss");
            String dateStr = df.format(new Date());
            pxXmlsDir = Paths.get(pcIssuesBasePath, dateStr, "pxXmls");
            pcIssuesDir = Paths.get(pcIssuesBasePath, dateStr, "pcIssues");
            errorsDir = Paths.get(pcIssuesBasePath, dateStr, "errors");

            if (!Files.exists(pxXmlsDir)) {
                pxXmlsDir = Files.createDirectories(pxXmlsDir);
            }
            if (!Files.exists(pcIssuesDir)) {
                pcIssuesDir = Files.createDirectories(pcIssuesDir);
            }
            if (!Files.exists(errorsDir)) {
                errorsDir = Files.createDirectories(errorsDir);
            }

            xmlParams = new XMLParams();
            xmlParams.setPxPartner(pxPartnerName);
            xmlParams.setAuthentication(pxPassword);
            xmlParams.setMethod("submitDataset");
            xmlParams.setTest("yes");
            xmlParams.setNoEmailBroadcast("true");
            xmlParams.setVerbose("no");

            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Job pcIssuesJob() {
        return jobBuilderFactory
                .get("pcIssuesJob")
                .start(stepBuilderFactory
                        .get("initPcIssuesJob")
                        .tasklet(initPcIssuesJob(null))
                        .build())
                .next(generatePxXmlStep())
                .next(pcIssuesPrintTraceStep())
                .build();
    }


    @Bean
    public Step generatePxXmlStep() {

        return stepBuilderFactory
                .get("generatePxXmlMessageStep")
                .tasklet((stepContribution, chunkContext) -> {

                    long startTime = System.currentTimeMillis();

                    log.info("Generating PX XML in " + pxXmlsDir.toString());

                    if (projectAccessions == null || projectAccessions.length == 0) {
                        oracleProjectRepository.findAll().forEach(this::generatePxXml);
                    } else {
                        Arrays.stream(projectAccessions).forEach(a -> {
                            Project oracleProject = oracleProjectRepository.findByAccession(a);
                            generatePxXml(oracleProject);
                        });
                    }
                    taskTimeMap.put("generatePxXmlMessageStep", System.currentTimeMillis() - startTime);
                    return RepeatStatus.FINISHED;
                }).build();
    }

    private void generatePxXml(Project project) {
        String accession = project.getAccession();
        log.info("Processing: " + accession);
        try {
            if (!accession.startsWith("PXD") || !project.isPublicProject()) {
                return;
            }

//            String testAccession = accession.replaceAll("PXD","PXT"); //PC repotrs this error: message=The dataset identifier "PXD006905" must be PXTnnnnnn not PXDnnnnnn if test=yes

            DateFormat df = new SimpleDateFormat("yyyy/MM");
            String datasetPath = df.format(project.getPublicationDate()) + "/" + accession;
//            String testDatasetPath = df.format(project.getPublicationDate()) + "/" + testAccession;

            File pxSubmissionSummaryFile = Paths.get(prideDataPath, datasetPath, "internal", "submission.px").toFile();
            File pxXml = pxXmlMessageWriter.createIntialPxXml(pxSubmissionSummaryFile, pxXmlsDir.toFile(), accession, datasetPath, pxSchemaVersion);

            String pxXmlAbsolutePath = pxXml.getAbsolutePath();
            log.info("Validating PX file: " + pxXmlAbsolutePath);
            String output = ValidateMessage.validateMessage(pxXml, pxSchemaVersion);
            if (StringUtils.isNotEmpty(output)) {
                log.error(output);
                throw new IllegalStateException("PX file not valid :" + pxXmlAbsolutePath + "\n" + output);
            } else {
                log.info("PX file is valid: " + accession);
            }

            postPxXML(pxXml, accession);

        } catch (Exception e) {
            log.error(e.getMessage(), e);
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            e.printStackTrace(pw);
            try {
                Files.write(Paths.get(errorsDir.toString(), accession), sw.toString().getBytes(),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND); //store error to a file as well
            } catch (Exception ex) {
                log.error(ex.getMessage(), ex);
            }
        }
    }

    private void postPxXML(File pxXml, String accession) throws IOException {
        log.info("Submitting PX XML to ProteomeCentral : " + accession);

        String response = PostMessage.postFile(pxXml, xmlParams, pxSchemaVersion);
        if (response != null) {
            log.info("ProteomeCentral response message for posting PX XML : {}\n {}", accession, response);
            String responseLowerCase = response.toLowerCase();
            if (responseLowerCase.contains("result=error") ||
                    responseLowerCase.contains("message=error") ||
                    responseLowerCase.contains("result=foundvalidationerrors") ||
                    responseLowerCase.contains("internal server error")
            ) {
                if(!responseLowerCase.contains("must be pxtnnnnnn not pxdnnnnnn if test=yes")) { //this is OK case
                    Files.write(Paths.get(pcIssuesDir.toString(), accession), response.getBytes());
                }
            }
        } else {
            String s = "No response (null) from ProteomeCentral when submitting PX XML: " + accession;
            log.error(s);
            Files.write(Paths.get(errorsDir.toString(), accession), s.getBytes(),
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND); //store error to a file as well
        }
    }

    @Bean
    public Step pcIssuesPrintTraceStep() {
        return stepBuilderFactory
                .get("pcIssuesPrintTraceStep")
                .tasklet((stepContribution, chunkContext) -> {
                    taskTimeMap.forEach((key, value) -> log.info("Task: " + key + " Time: " + value));
                    return RepeatStatus.FINISHED;
                }).build();
    }
}
