package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.dataprovider.reference.Reference;
import uk.ac.ebi.pride.archive.dataprovider.reference.ReferenceProvider;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.model.projects.ReanalysisProject;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideReanalysisMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;
import uk.ac.ebi.pride.pubmed.PubMedFetcher;
import uk.ac.ebi.pride.pubmed.model.EupmcReferenceSummary;

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
 * Created by Suresh Hewapathirana (sureshhewabi@gmail.com)
 */
@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class, DataSourceConfiguration.class, ArchiveMongoConfig.class})
public class PrideArchiveReanalysisJob extends AbstractArchiveJob {

    private final PrideReanalysisMongoService prideReanalysisMongoService;

    @Autowired
    public PrideArchiveReanalysisJob(PrideReanalysisMongoService prideReanalysisMongoService) {
        this.prideReanalysisMongoService = prideReanalysisMongoService;
    }

    /**
     * This Step reads re-analysis data from the TSV and updates in the MongoDB collection
     * @return Step
     */
    @Bean
    public Step updateReanalysisDatasetsStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_UPDATE_REANALYSIS_DATA.name())
                .tasklet((stepContribution, chunkContext) -> {
                    HashMap<String, Set<String>> reanalysisDatasets = extractReferencesFromTsv();

                    // get references from Pubmed
                    HashMap<String, Set<EupmcReferenceSummary>> reannalysisReferences = new HashMap<>();
                    for (Map.Entry<String, Set<String>> entry : reanalysisDatasets.entrySet()) {
                        Set<EupmcReferenceSummary> eupmcReferenceSummarySet = new HashSet<>();
                        for (String pubmedId:entry.getValue()) {
                            EupmcReferenceSummary eupmcReferenceSummary = PubMedFetcher.getPubMedSummary(pubmedId);
                            eupmcReferenceSummarySet.add(eupmcReferenceSummary);
                        }
                        reannalysisReferences.put(entry.getKey(),eupmcReferenceSummarySet);
                    }
                    updateReanalysisData(reannalysisReferences);
                    return RepeatStatus.FINISHED;
                }).build();
    }

    /**
     * This methods reads a TSV file from the github, filter the re-analyse dataset and then extract the references for
     * individual project
     *
     * @return Set of references for each project
     */
    private HashMap<String, Set<String>> extractReferencesFromTsv(){

        HashMap<String, Set<String>> reanalysisDatasets = new HashMap<>();
        final String stringURL = "https://raw.githubusercontent.com/PRIDE-Utilities/pride-ontology/master/pride-annotations/project-citations.csv";
        URL url;
        try {
            url = new URL(stringURL);
            File fileTemp = File.createTempFile("project-citations", ".csv");
            FileUtils.copyURLToFile(url, fileTemp);
            String line;
            BufferedReader br = new BufferedReader(new FileReader(fileTemp));
            br.readLine(); // read the header
            while ((line = br.readLine()) != null) {

                // use comma as separator
                String[] dataset = line.split(",");
                if(dataset.length == 3){
                    // filter only Re-analysis records
                    String[] citationTypes;
                    if(dataset[1].contains("|")){
                        citationTypes = dataset[1].split("\\|");
                    }else{
                        citationTypes = new String[]{dataset[1].trim()};
                    }

                    for (String citationType:citationTypes) {
                        if(citationType.toLowerCase().contains("re-analysis") || (citationType.toLowerCase().contains("reanalysis")) ){
                            String[] projectAccessions;
                            if(dataset[2].contains("|")){
                                projectAccessions = dataset[2].split("\\|");
                            }else{
                                projectAccessions = new String[]{dataset[2].trim()};
                            }

                            for (String projectAccession:projectAccessions) {
                                if(reanalysisDatasets.containsKey(projectAccession.trim())){
                                    Set<String> publications = reanalysisDatasets.get(projectAccession.trim());
                                    publications.add(dataset[0].trim());
                                    reanalysisDatasets.put(projectAccession.trim(), publications);
                                }else{
                                    Set<String> publications = new HashSet<>();
                                    publications.add(dataset[0].trim());
                                    reanalysisDatasets.put(projectAccession.trim(), publications);
                                }
                            }
                        }
                    }
                }
            }
            fileTemp.deleteOnExit();

        } catch (Exception e) {
            log.error("Error reading project-citations file from --" + stringURL + e.getMessage());
        }
        return reanalysisDatasets;
    }

    /**
     * This method updates the database
     *
     * @param reanalysisDatasets Key is the project accession and value is set of references to the pubmed
     */
    private void updateReanalysisData(HashMap<String, Set<EupmcReferenceSummary>> reanalysisDatasets) {

        try {
            for (Map.Entry<String, Set<EupmcReferenceSummary>> entry : reanalysisDatasets.entrySet()) {

                Set<ReferenceProvider> referenceProviders = new HashSet<>();
                for (EupmcReferenceSummary eupmcReferenceSummary:entry.getValue()) {
                    ReferenceProvider referenceProvider = new Reference(
                            eupmcReferenceSummary.getRefLine(),
                            Integer.parseInt(eupmcReferenceSummary.getEupmcResult().getPmid()),
                            eupmcReferenceSummary.getEupmcResult().getDoi());
                    referenceProviders.add(referenceProvider);
                }
                ReanalysisProject reanalysisProject = ReanalysisProject.builder()
                        .accession(entry.getKey())
                        .references(referenceProviders)
                        .build();
                prideReanalysisMongoService.upsert(reanalysisProject);

            }
        } catch (Exception e) {
            log.error("Error while updating the re-analysis data to MongoDB : " + e.getMessage());
        }
    }

    /**
     * This job read re-analysis data from a file in github and update the re-analysis collection in MongoDB
     * @return
     */
    @Bean
    public Job syncReanalysisDatasetsToMongoDBJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_REANALYSIS_INFO_UPDATE.getName())
                .start(updateReanalysisDatasetsStep())
                .build();
    }
}
