package uk.ac.ebi.pride.archive.pipeline.jobs.projects;


import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.*;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveOracleConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.archive.repo.repos.project.Project;
import uk.ac.ebi.pride.archive.repo.repos.project.ProjectRepository;
import uk.ac.ebi.pride.integration.command.builder.CommandBuilder;
import uk.ac.ebi.pride.integration.command.builder.DefaultCommandBuilder;
import uk.ac.ebi.pride.integration.command.runner.CommandRunner;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * @author Suresh Hewapathirana
 */
@Configuration
@Slf4j
@PropertySource("classpath:application.properties")
@Import({ArchiveOracleConfig.class,  DataSourceConfiguration.class})
@ImportResource({"classpath*:/META-INF/spring/integration/common-context.xml"})
public class SyncMissingProjectsWithProteomeXJob extends AbstractArchiveJob {

    @Autowired
    ProjectRepository oracleRepository;

    @Value("${command.update.pxxml.command}")
    private String assayAnalyseCommand;

    @Value("${proteome.exchange.url}")
    private String proteomeXchangeURL;

    @Autowired
    private CommandRunner commandRunner;

    /**
     * Defines the job to Sync all missing public projects from OracleDB into ProteomeXchange database.
     *
     * @return the  job
     */
    @Bean
    public Job syncMissingProjectsOracleToPXJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_SYNC_MISSING_PROJECTS_ORACLE_PC.getName())
                .start(syncMissingProjectsOracleToPXStep())
                .build();
    }

    @Bean
    Step syncMissingProjectsOracleToPXStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MISSING_PROJ_ORACLE_TO_PC_SYNC.name())
                .tasklet(
                        (stepContribution, chunkContext) -> {
                            Set<String> pxPublicAccessions = getProteomXchangeData();
                            log.info("Number of ProteomeXchange projects: " + pxPublicAccessions.size());
                            Set<String> oraclePublicAccessions = getOracleProjectAccessions();
                            log.info("Number of oraclePublicAccessions projects: " + oraclePublicAccessions.size());

                            for (String prideAccession : oraclePublicAccessions) {
                                if(!pxPublicAccessions.contains(prideAccession)){
                                    if (!prideAccession.startsWith("PRD")) {
                                        log.info(prideAccession + " is not public on ProteomeXchange");
                                        runCommand(prideAccession);
                                    }
                                }
                            }
                            return RepeatStatus.FINISHED;
                        })
                .build();
    }

    private Set<String> getProteomXchangeData() {

        final String URI = proteomeXchangeURL;

        Set<String> accessions = new HashSet<>();

        RestTemplate restTemplate = new RestTemplate();
        String records = restTemplate.getForObject(URI, String.class);
        String[] recordsTemp = records.split("GetDataset\\?ID=");
        for (int i = 1; i < recordsTemp.length; i++) {
            String accession = recordsTemp[i].split("\" target")[0];
            if (!accession.startsWith("PRD")) {
                if (accession.startsWith("PXD")) {
                    accessions.add(accession.substring(0, 9));
                } else {
                    accessions.add(accession.substring(0, 10));
                }
            }
        }
        return accessions;
    }

    /**
     * Connect to Oracle Database and get project accessions of all the public projects
     * (project with old PRD accessions)
     *
     * @return Set of project accessions
     */
    private Set<String> getOracleProjectAccessions(){

        Iterable<Project> oracleAllProjects = oracleRepository.findAll();
        Set<String> oracleAccessions = StreamSupport.stream(oracleAllProjects.spliterator(), false)
                .filter(Project::isPublicProject)
                .map(Project::getAccession)
                .collect(Collectors.toSet());

        log.info( "Number of Oracle projects: "+ oracleAccessions.size());
        return oracleAccessions;
    }

    /**
     * Run command to launch job to lsf
     */
    private void runCommand(String accession) {

        CommandBuilder commandBuilder = new DefaultCommandBuilder();

        commandBuilder.argument(assayAnalyseCommand);
        // append project accession
        commandBuilder.argument("-a", accession);

        Collection<String> command = commandBuilder.getCommand();
        log.info(command.toString());

        commandRunner.run(command);
    }
}