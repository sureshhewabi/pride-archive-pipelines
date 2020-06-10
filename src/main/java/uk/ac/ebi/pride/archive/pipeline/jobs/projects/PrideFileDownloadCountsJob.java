package uk.ac.ebi.pride.archive.pipeline.jobs.projects;

import com.google.common.collect.Multiset;
import lombok.extern.slf4j.Slf4j;
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
import uk.ac.ebi.ddi.downloas.logs.ElasticSearchWsClient;
import uk.ac.ebi.ddi.downloas.logs.ElasticSearchWsConfigProd;
import uk.ac.ebi.pride.archive.dataprovider.param.CvParam;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.configuration.ElasticSearchWsConfig;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.model.files.MongoPrideFile;
import uk.ac.ebi.pride.mongodb.archive.service.files.PrideFileMongoService;
import uk.ac.ebi.pride.mongodb.configs.ArchiveMongoConfig;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

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
@Import({ArchiveMongoConfig.class, DataSourceConfiguration.class, ArchiveMongoConfig.class, ElasticSearchWsConfig.class})
public class PrideFileDownloadCountsJob extends AbstractArchiveJob {

    @Autowired
    PrideFileMongoService prideFileMongoService;

    @Autowired
    ElasticSearchWsClient elasticSearchClient;

    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private String startDate;
    private String endDate;

    @Bean
    @StepScope
    public Tasklet initJobPrideFileDownloadCountsJob(@Value("#{jobParameters['startDate']}") String startDate, @Value("#{jobParameters['endDate']}") String endDate) {
        return (stepContribution, chunkContext) ->
        {
            this.startDate = startDate;
            this.endDate = endDate;
            return RepeatStatus.FINISHED;
        };
    }

    /**
     * This Step reads re-analysis data from the TSV and updates in the MongoDB collection
     * @return Step
     */
    @Bean
    public Step updateFileDownloadCountsStep() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_UPDATE_FILES_DOWNLOAD_COUNTS.name())
                .tasklet((stepContribution, chunkContext) -> {

                    Date fromDate = dateFormat.parse(this.startDate);
                    Date toDate = dateFormat.parse(this.endDate);

                    Map<String,Map<String, Integer>> projectFileDownloadCounts = getPrideDownloads(fromDate, toDate);
                    updateFileDownloadCounts(projectFileDownloadCounts);
                    System.out.println(projectFileDownloadCounts.size());
                    return RepeatStatus.FINISHED;
                }).build();
    }

    /**
     * Update the download counts of individual files in MongoDB
     * @param projectFileDownloadCounts
     */
    private void updateFileDownloadCounts( Map<String,Map<String, Integer>> projectFileDownloadCounts) {

        try {
            for(Map.Entry<String,Map<String, Integer>> projectMap : projectFileDownloadCounts.entrySet().stream().filter(stringMapEntry -> stringMapEntry.getKey().equals("PXD013868")).collect(Collectors.toList())){
                List<MongoPrideFile> mongoPrideFiles = prideFileMongoService.findFilesByProjectAccession(projectMap.getKey());
                for(Map.Entry<String, Integer> fileMap : projectMap.getValue().entrySet()){
                    for (MongoPrideFile mongoPrideFile : mongoPrideFiles){
                        if(mongoPrideFile.getFileName().equals(fileMap.getKey())){
                            Set<CvParam> cvParamSet;
                            if(mongoPrideFile.getAdditionalAttributes() != null){
                                cvParamSet = mongoPrideFile.getAdditionalAttributes();
                            }else{
                                cvParamSet = new HashSet<>();
                            }
                            //TODO: I have temporarily given an accession until OLS Approve my request
                            cvParamSet.add(new CvParam("PRIDE", "PRIDE0000123", "Number of Downloads", fileMap.getValue().toString()));
                            mongoPrideFile.setAdditionalAttributes(cvParamSet);
                            prideFileMongoService.save(mongoPrideFile);
                            break;
                        }
                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * Read logs and retrieve number of downloads for each file in each PRIDE project
     *
     * @return
     * @throws ParseException
     */
    private Map<String,Map<String, Integer>> getPrideDownloads(Date fromDate, Date toDate) throws ParseException {

        Map<String,Map<String, Integer>> projectFileDownloadCounts = new HashMap<>();
        long startTime = System.nanoTime();

        elasticSearchClient.initialiseData(fromDate, toDate);

        // ACCESSION_TO_PERIOD_TO_ANONYMISED_IP_ADDRESS_TO_FILE_NAME
        Map<String, Map<String, Map<String, Multiset<String>>>> dbDownloadInfo
                = elasticSearchClient.getDownloadsData(ElasticSearchWsConfigProd.DB.Pride);
        System.out.println(dbDownloadInfo.size());

        try {
            // logic how we calculate number of downloads for each file
            for (Map.Entry<String, Map<String, Map<String, Multiset<String>>>> projectMap : dbDownloadInfo.entrySet()){
                Map<String, Integer> fileDownloadCounts = new HashMap<>();
                for (Map.Entry<String, Map<String, Multiset<String>>> ipAddressMap : projectMap.getValue().entrySet()) {
                    for (Map.Entry<String, Multiset<String>> fileMap : ipAddressMap.getValue().entrySet()) {
                        for (String file : fileMap.getValue()) {
                            if (fileDownloadCounts.containsKey(file))
                                fileDownloadCounts.put(file, fileDownloadCounts.get(file) + 1);
                            else fileDownloadCounts.put(file, 1);
                        }
                    }
                }
                projectFileDownloadCounts.put(projectMap.getKey(), fileDownloadCounts);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        long stopTime = System.nanoTime();
        long convert = TimeUnit.MINUTES.convert(stopTime - startTime, TimeUnit.NANOSECONDS);
        System.out.println(convert + " minutes took to calculate file download counts");
        return projectFileDownloadCounts;
    }

    /**
     * This job reads the logs and count number of downloads of each file in the dataset and add/update in
     * MongoDB file collection as an additional attribute
     * @return
     */
    @Bean
    public Job fileDownloadCountsJob() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_FILES_DOWNLOAD_COUNTS.getName())
                .start(stepBuilderFactory
                        .get("initJobPrideFileDownloadCountsJob")
                        .tasklet(initJobPrideFileDownloadCountsJob(null, null))
                        .build())
                .next(updateFileDownloadCountsStep())
                .build();
    }
}