package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveMongoConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.*;

/**
 * This code is licensed under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * ==Overview==
 * <p>
 * This class
 * <p>
 * Created by ypriverol (ypriverol@gmail.com) on 03/08/2018.
 */
@Configuration
@Slf4j
@Import({ArchiveMongoConfig.class, DataSourceConfiguration.class})
public class PrideProjectsAnnotateCountryFromGitHubJob extends AbstractArchiveJob {


    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    HashMap<String, Set<String>> countries;

    @Bean
    public HashMap<String, Set<String>> parseCountries() {
        this.countries = new HashMap<>();
        String stringURL = "https://raw.githubusercontent.com/PRIDE-Utilities/pride-ontology/master/pride-annotations/px-countries.csv";
        URL url;
        try {
            url = new URL(stringURL);
            File fileTemp = File.createTempFile("countries", ".csv");
            FileUtils.copyURLToFile(url, fileTemp);
            String line = "";
            BufferedReader br = new BufferedReader(new FileReader(fileTemp));
            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] country = line.split(",");
                if(country.length == 2){
                    String projectID = country[0].trim();
                    String countryValue = country[1].trim().replaceAll("\"", "");
                    Set<String> values = countries.containsKey(projectID)?countries.get(projectID):new HashSet<>();
                    values.add(countryValue);
                    countries.put(projectID, values);
                }
            }
            fileTemp.deleteOnExit();

            } catch (java.io.IOException e) {
            log.error("Error reading country file from --" + stringURL + e.getMessage());
        }
       return countries;
    }


    @Bean
    Step annotateProjectsWithCountry() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_MONGODB_ANNOTATE_PROJECTS_COUNTRY.name())
                .tasklet((stepContribution, chunkContext) -> {
                    prideProjectMongoService.findAllStream().forEach( mongoPrideProject ->{

                        if(countries.containsKey(mongoPrideProject.getAccession())){
                            mongoPrideProject.setCountries(new ArrayList<>(countries.get(mongoPrideProject.getAccession())));
                            prideProjectMongoService.update(mongoPrideProject);
                            log.info("The project -- " + mongoPrideProject.getAccession() + " has been updated in MongoDB");
                        }
                    });
                    return RepeatStatus.FINISHED;
                })
                .build();
    }


    /**
     * Defines the job to Sync all the projects from OracleDB into MongoDB database.
     *
     * @return the calculatePrideArchiveDataUsage job
     */
    @Bean
    public Job annotateProjectsWithCountryJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_MONGODB_ANNOTATE_PROJECTS_COUNTRY.getName())
                .start(annotateProjectsWithCountry())
                .build();
    }


}
