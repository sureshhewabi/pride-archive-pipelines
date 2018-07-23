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
import uk.ac.ebi.pride.archive.dataprovider.param.ParamProvider;
import uk.ac.ebi.pride.archive.dataprovider.utils.Tuple;
import uk.ac.ebi.pride.archive.pipeline.configuration.ArchiveMongoConfig;
import uk.ac.ebi.pride.archive.pipeline.configuration.DataSourceConfiguration;
import uk.ac.ebi.pride.archive.pipeline.jobs.AbstractArchiveJob;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.mongodb.archive.model.param.MongoCvParam;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;
import uk.ac.ebi.pride.mongodb.archive.model.stats.PrideStatsKeysConstants;
import uk.ac.ebi.pride.mongodb.archive.service.projects.PrideProjectMongoService;
import uk.ac.ebi.pride.mongodb.archive.service.stats.PrideStatsMongoService;
import uk.ac.ebi.pride.utilities.term.CvTermReference;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

    private Date date;

    /**
     * All the stats are compute at an specific time 00:00:00
     *
     */
    @Autowired
    public void initDate() {
        Calendar now = Calendar.getInstance();
        now.set(Calendar.HOUR, 0);
        now.set(Calendar.MINUTE, 0);
        now.set(Calendar.SECOND, 0);
        now.set(Calendar.HOUR_OF_DAY, 0);
        this.date = now.getTime();
    }

    /**
     * This method estimate the number of submissions per year. The method stored in the database the final results.
     * @return @{@link Step}
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

    /**
     * This method estimate the number of submissions per month. The method stored in the database the final results of the
     * metrics.
     * @return @{@link Step}
     */
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
                    prideStatsMongoService.updateSubmissionCountStats(date, PrideStatsKeysConstants.SUBMISSIONS_PER_MONTH, submissionsByDate);
                    return RepeatStatus.FINISHED;

                })
                .build();
    }


    /**
     * This method estimate the number of submissions by Instrument name.
     * @return @{@link Step}
     */
    @Bean
    public Step estimateInstrumentsCount() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SUBMISSION_STATS_INSTRUMENT.name())
                .tasklet((stepContribution, chunkContext) -> {
                    List<Tuple<String, Integer>> submissionsByDate = prideProjectMongoService
                            .findAllStream()
                            .flatMap(x -> x.getInstrumentsCvParams().stream())
                            .collect(Collectors.groupingBy(ParamProvider::getName))
                            .entrySet()
                            .stream()
                            .map(x -> new Tuple<String, Integer>(x.getKey(), x.getValue().size()))
                            .sorted((x, y) -> y.getValue().compareTo(x.getValue()))
                            .collect(Collectors.toList());
                    prideStatsMongoService.updateSubmissionCountStats(date, PrideStatsKeysConstants.SUBMISSIONS_PER_INSTRUMENTS, submissionsByDate);
                    return RepeatStatus.FINISHED;

                })
                .build();
    }

    /**
     * This method estimate the number of submissions by Organism name.
     * @return @{@link Step}
     */
    @Bean
    public Step estimateOrganismCount() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SUBMISSION_STATS_ORGANISM.name())
                .tasklet((stepContribution, chunkContext) -> {
                    List<Tuple<String, Integer>> submissionsByDate = estimateDatasetsByTermInSampleDescription(prideProjectMongoService.findAllStream(), CvTermReference.EFO_ORGANISM);
                    prideStatsMongoService.updateSubmissionCountStats(date, PrideStatsKeysConstants.SUBMISSIONS_PER_ORGANISM, submissionsByDate);
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    /**
     * This method estimate the number of submissions by Organism part.
     * @return @{@link Step}
     */
    @Bean
    public Step estimateOrganismPartCount() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SUBMISSION_STATS_ORGANISM_PART.name())
                .tasklet((stepContribution, chunkContext) -> {
                    List<Tuple<String, Integer>> submissionsByDate = estimateDatasetsByTermInSampleDescription(prideProjectMongoService.findAllStream(), CvTermReference.EFO_ORGANISM_PART);
                    prideStatsMongoService.updateSubmissionCountStats(date, PrideStatsKeysConstants.SUBMISSIONS_PER_ORGANISM_PART, submissionsByDate);
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    /**
     * This method estimate the number of submissions by Diseases.
     * @return @{@link Step}
     */
    @Bean
    public Step estimateDiseasesCount() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SUBMISSION_STATS_DISEASES.name())
                .tasklet((stepContribution, chunkContext) -> {
                    List<Tuple<String, Integer>> submissionsByDate = estimateDatasetsByTermInSampleDescription(prideProjectMongoService.findAllStream(), CvTermReference.EFO_DISEASE);
                    prideStatsMongoService.updateSubmissionCountStats(date, PrideStatsKeysConstants.SUBMISSIONS_PER_DISEASES, submissionsByDate);
                    return RepeatStatus.FINISHED;
                })
                .build();
    }

    /**
     * This method estimate the number of submissions by Diseases.
     * @return @{@link Step}
     */
    @Bean
    public Step estimateModificationCount() {
        return stepBuilderFactory
                .get(SubmissionPipelineConstants.PrideArchiveStepNames.PRIDE_ARCHIVE_SUBMISSION_STATS_MODIFICATIONS.name())
                .tasklet((stepContribution, chunkContext) -> {
                    List<Tuple<String, Integer>> submissionsByDate = prideProjectMongoService
                            .findAllStream()
                            .flatMap( x-> x.getPtmList().stream())
                            .collect(Collectors.groupingBy(MongoCvParam::getName))
                            .entrySet()
                            .stream()
                            .map( x -> new Tuple<String, Integer>(x.getKey() , x.getValue().size()))
                            .sorted((x,y) -> y.getValue().compareTo(x.getValue()))
                            .collect(Collectors.toList());
                    prideStatsMongoService.updateSubmissionCountStats(date, PrideStatsKeysConstants.SUBMISSIONS_PER_MODIFICATIONS, submissionsByDate);
                    return RepeatStatus.FINISHED;
                })
                .build();
    }


    /**
     * This job estimates different statistics around each submission.
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
                .next(estimateOrganismCount())
                .next(estimateModificationCount())
                .next(estimateOrganismPartCount())
                .next(estimateDiseasesCount())
                .build();
    }

    /**
     * Estimate the number of datasets for an specific {@link CvTermReference} in the sample Description.
     * @param projects PRIDE projects
     * @param term {@link CvTermReference}
     * @return List of Tuple with the values.
     */
    private List<Tuple<String, Integer>> estimateDatasetsByTermInSampleDescription(Stream<MongoPrideProject> projects, CvTermReference term){
        return projects.map(a -> a.getSamplesDescription())
                .collect(Collectors.toList())
                .stream().flatMap(b -> b.stream())
                .collect(Collectors.toList())
                .stream()
                .filter(c -> c.getKey().getAccession().equalsIgnoreCase(term.getAccession()))
                .map(d -> d.getValue())
                .collect(Collectors.toList())
                .stream()
                .flatMap(e -> e.stream())
                .collect(Collectors.groupingBy(MongoCvParam::getName))
                .entrySet()
                .stream()
                .map(f -> new Tuple<String, Integer>(f.getKey(), f.getValue().size()))
                .sorted((x,y) -> {
                    return y.getValue().compareTo(x.getValue());
                })
                .collect(Collectors.toList());
    }


}
