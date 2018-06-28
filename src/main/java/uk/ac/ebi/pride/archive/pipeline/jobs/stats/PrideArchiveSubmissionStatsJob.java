package uk.ac.ebi.pride.archive.pipeline.jobs.stats;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.pride.archive.dataprovider.utils.Tuple;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveMongoConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.model.stats.PrideStatsKeysConstants;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.stats.PrideStatsMongoService;

import java.text.SimpleDateFormat;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
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
 * Created by ypriverol (ypriverol@gmail.com) on 27/06/2018.
 */
@Configuration
@Slf4j
@EnableBatchProcessing
@Import({ArchiveMongoConfig.class, DataSourceConfiguration.class})
public class PrideArchiveSubmissionStatsJob extends AbstractArchiveJob {

    @Autowired
    PrideProjectMongoService prideProjectMongoService;

    @Autowired
    PrideStatsMongoService prideStatsMongoService;

    Date date;

    @Autowired
    public void initDate() {
        this.date = new Date();
    }

    /**
     * This methods connects to the database read all the Oracle information for public
     * @return
     */
    @Bean
    Step estimateSubmissionByYear() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SUBMISSION_STATS_YEAR.name())
                .tasklet((stepContribution, chunkContext) -> {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY");
                    List<Tuple<String, Integer>> submissionsByDate = prideProjectMongoService
                            .findAllStream()
                            .collect(Collectors
                                    .groupingBy(item -> simpleDateFormat.format(item.getSubmissionDate()).toUpperCase()))
                            .entrySet()
                            .stream()
                            .map( x-> new Tuple<>(x.getKey(), x.getValue().size()))
                            .sorted(Comparator.comparingInt(x -> Integer.parseInt(x.getKey())))
                            .collect(Collectors.toList());
                    prideStatsMongoService.updateSubmissionCountStats(date, PrideStatsKeysConstants.SUBMISSIONS_PER_YEAR, submissionsByDate);
                    return RepeatStatus.FINISHED;

                })
                .build();
    }

    @Bean
    public Step estimateSubmissionByMonth() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SUBMISSION_STATS_MONTH.name())
                .tasklet((stepContribution, chunkContext) -> {
                    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM");
                    List<Tuple<String, Integer>> submissionsByDate = prideProjectMongoService
                            .findAllStream()
                            .collect(Collectors
                                    .groupingBy(item -> simpleDateFormat.format(item.getSubmissionDate()).toUpperCase()))
                            .entrySet()
                            .stream()
                            .map( x-> new Tuple<>(x.getKey(), x.getValue().size()))
                            .sorted((x,y) -> StringUtils.compare(x.getKey(),y.getKey()))
                            .collect(Collectors.toList());
                    prideStatsMongoService.updateSubmissionCountStats(date, PrideStatsKeysConstants.SUBMISSIONS_PER_YEAR, submissionsByDate);
                    return RepeatStatus.FINISHED;

                })
                .build();
    }


    @Bean
    public Step estimateInstrumentsCount() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SUBMISSION_STATS_INSTRUMENT.name())
                .tasklet((stepContribution, chunkContext) -> {
                    List<Tuple<String, Integer>> submissionsByDate = prideProjectMongoService
                            .findAllStream()
                            .flatMap( x-> x.getInstrumentsCvParams().stream())
                            .collect(Collectors.groupingBy( x -> x.getName()))
                            .entrySet()
                            .stream()
                            .map( x -> new Tuple(x.getKey() , x.getValue().size()))
                            .sorted((x,y) -> Integer.compare((Integer) x.getValue(), (Integer) x.getValue()))
                            .collect(Collectors.toList());
                    prideStatsMongoService.updateSubmissionCountStats(date, PrideStatsKeysConstants.SUBMISSIONS_PER_INSTRUMENTS, submissionsByDate);
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
    public Job computeSubmissionStats() {
        return jobBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveJobNames.PRIDE_ARCHIVE_SUBMISSION_STATS.getName())
                .start(estimateSubmissionByYear())
                .next(estimateSubmissionByMonth())
                .next(estimateInstrumentsCount())
                .build();
    }




}
