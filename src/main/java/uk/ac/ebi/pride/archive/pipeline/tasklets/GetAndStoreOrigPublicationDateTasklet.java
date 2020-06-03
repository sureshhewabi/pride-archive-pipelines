package uk.ac.ebi.pride.archive.pipeline.tasklets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;

import java.io.File;
import java.util.Calendar;
import java.util.Date;

@Component
public class GetAndStoreOrigPublicationDateTasklet extends AbstractTasklet {

    public static final Logger logger = LoggerFactory.getLogger(GetAndStoreOrigPublicationDateTasklet.class);

    public static final String PUBLIC_PATH_FRAGMENT = "public.path.fragment";


    @Value("${project.accession:null}")
    private String projectAccession;

    @Autowired
    private PrideProjectMongoService prideProjectMongoService;

    private String pubMedId = " ";
    private String doi = "";

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        logger.info("Getting and setting publication date as the originally defined.");
        MongoPrideProject project = prideProjectMongoService.findByAccession(projectAccession).get();
        Assert.notNull(project, "Project to update cannot be null! Accession: " + projectAccession);
        Assert.isTrue(project.isPublicProject(), "Project to update cannot be private! Accession: " + projectAccession);
        boolean hasReference = pubMedId != null || doi != null;
        Assert.isTrue(hasReference, "Pubmed and doi both cannot be null!");
        String publicPath = GetAndStoreOrigPublicationDateTasklet.getPubPathFrag(project.getPublicationDate(), projectAccession);
        updateStepExecutionContext(chunkContext, PUBLIC_PATH_FRAGMENT, publicPath);

        return RepeatStatus.FINISHED;
    }

    public static String getPubPathFrag(Date publicationDate, String projectAccession) {
        logger.info("Generating public file path fragment based on the publication date and project accession");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(publicationDate);
        int month = calendar.get(Calendar.MONTH) + 1; // the month are zero based, hence the correction +1
        int year = calendar.get(Calendar.YEAR);
        String datePath = year + File.separator + (month < 10 ? "0" : "") + month;
        String publicPath = datePath + File.separator + projectAccession;
        logger.info("Generated public path fragment: " + publicPath);
        return publicPath;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        Assert.notNull(prideProjectMongoService, "PrideProjectMongoService cannot be null");
        Assert.notNull(projectAccession, "Project accession cannot be null");
    }

    public void setProjectAccession(String projectAccession) {
        this.projectAccession = projectAccession;
    }

    public void setProjectRepository(PrideProjectMongoService prideProjectMongoService) {
        this.prideProjectMongoService = prideProjectMongoService;
    }

    public void setPubMedId(String pubmed) {
        this.pubMedId = pubmed;
    }

    public void setDoi(String doi) {
        this.doi = doi;
    }

}