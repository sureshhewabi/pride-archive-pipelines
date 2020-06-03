package uk.ac.ebi.pride.archive.pipeline.tasklets;

import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.Assert;
import org.springframework.util.CollectionUtils;
import uk.ac.ebi.pride.archive.repo.repos.project.ProjectRepository;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

@Slf4j
public class LaunchIndividualEbeyeXmlTasklet extends AbstractTasklet {

    private String projectAccession;
    private PrideProjectMongoService projectRepository;

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        List<String> exceptionsLaunchingProjects = Collections.synchronizedList(new ArrayList<String>());
        launchIndividualEbeyeXmlGenerationForProjectAcc(projectAccession, projectRepository, exceptionsLaunchingProjects);
        checkForExceptions(exceptionsLaunchingProjects);
        return RepeatStatus.FINISHED;
    }

    /**
     * This method launches an individual EBeye XML generation job for a public project.
     *
     * @param projectAcc the project's accession number to potentially generate EBeye XML for
     */
    static void launchIndividualEbeyeXmlGenerationForProjectAcc(String projectAcc, PrideProjectMongoService projectRepository, List<String> exceptionsLaunchingProjects) {
        Optional<MongoPrideProject> project = projectRepository.findByAccession(projectAcc);
        if (project.get().isPublicProject()) {
            try {
                launchEBeyeXmlJob(projectAcc);
            } catch (IOException | InterruptedException e) {
                log.info("Problem launching job", e);
                exceptionsLaunchingProjects.add(projectAcc);
            }
        } else {
            log.info("Skipping private project: " + projectAcc);
        }
    }

    /**
     * Launches an individual EBeye generation job for a project.
     *
     * @param projectAcc the project accession
     * @throws IOException problems launching the EBeye generation job
     */
    private static void launchEBeyeXmlJob(String projectAcc) throws IOException, InterruptedException {
        log.info("Launching EBeye XML job for: " + projectAcc);
        String script = "./runEBeyeXMLGeneration.sh";
        log.info("Executing: $" + script + " -a " + projectAcc);
        Process p = new ProcessBuilder(script, "-a", projectAcc).start();
        p.waitFor();
        String line;
        InputStream inputStream = p.getInputStream();
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        while ((line = bufferedReader.readLine()) != null) {
            log.info(line);
        }
        bufferedReader.close();
        inputStream.close();
        if (p.exitValue() != 0) {
            inputStream = p.getErrorStream();
            log.error("Failed to launch individual EBeye generation job");
            bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
            while ((line = bufferedReader.readLine()) != null) {
                log.error(line);
            }
            bufferedReader.close();
            inputStream.close();
        }
    }

    /**
     * Checks if any exceptions have been caught, and logs the problematic accessions.
     *
     * @param exceptionsLaunchingProjects the list containing any problems launching EBeye generation jobs
     * @throws JobExecutionException problem with the job, the tasklet should exit
     */
    private static void checkForExceptions(List<String> exceptionsLaunchingProjects) throws JobExecutionException {
        if (!CollectionUtils.isEmpty(exceptionsLaunchingProjects)) {
            exceptionsLaunchingProjects.parallelStream().forEach(projAcc -> log.error("Problems launching EBeye generation for: " + projAcc));
            throw new JobExecutionException("Unable to launch individual EBeye generation jobs");
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(projectRepository, "Project repo cannot be null.");
        Assert.notNull(projectAccession, "Project accession cannot be null.");
    }

    public void setProjectAccession(String projectAccession) {
        this.projectAccession = projectAccession;
    }

    public void setProjectRepository(ProjectRepository projectRepository) {
        //this.projectRepository = projectRepository;
    }
}
