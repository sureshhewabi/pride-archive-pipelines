package uk.ac.ebi.pride.archive.pipeline.core.transformers;

import com.mongodb.Mongo;
import uk.ac.ebi.pride.archive.dataprovider.param.CvParamProvider;
import uk.ac.ebi.pride.archive.dataprovider.param.DefaultCvParam;
import uk.ac.ebi.pride.archive.dataprovider.reference.DefaultReference;
import uk.ac.ebi.pride.archive.dataprovider.reference.ReferenceProvider;
import uk.ac.ebi.pride.archive.dataprovider.user.ContactProvider;
import uk.ac.ebi.pride.archive.dataprovider.user.DefaultContact;
import uk.ac.ebi.pride.archive.dataprovider.utils.TitleConstants;
import uk.ac.ebi.pride.archive.pipeline.utility.StringUtils;
import uk.ac.ebi.pride.archive.repo.repos.project.*;
import uk.ac.ebi.pride.mongodb.archive.model.projects.MongoPrideProject;

import java.util.*;
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
 * Created by ypriverol (ypriverol@gmail.com) on 08/06/2018.
 */
public class PrideProjectTransformer {


    /**
     * An oracle project in pride will be converted into a MongoDB representation. Some of the information in the Oracle Project aare represented in a different way in
     * MongoDB.
     *
     * @param oracleProject Oracle PRIDE Project
     * @return MongoPrideProject
     */
    public static MongoPrideProject transformOracleToMongo(Project oracleProject) {

        // Before creating the project some complex data structures should be created

        // Get the Lab Heads
        List<ContactProvider> labHead = oracleProject.getLabHeads()
                .stream()
                .map(contactX -> new DefaultContact(TitleConstants.fromString(contactX.getTitle().getTitle()),
                        contactX.getFirstName(), contactX.getLastName(), contactX.getId().toString(), contactX.getAffiliation(),
                        contactX.getEmail(),  StringUtils.EMPTY_STRING, StringUtils.EMPTY_STRING))
                .collect(Collectors.toList());


        // Get the Submitters data
        List<ContactProvider> submitters = Collections.singletonList(new DefaultContact(TitleConstants.fromString(oracleProject.getSubmitter().getTitle().getTitle()),
                oracleProject.getSubmitter().getFirstName(), oracleProject.getSubmitter().getLastName(), oracleProject.getSubmitter().getId().toString(),
                oracleProject.getSubmitter().getAffiliation(),oracleProject.getSubmitter().getEmail(), StringUtils.EMPTY_STRING, StringUtils.EMPTY_STRING));

        // Get Instruments information
        List<CvParamProvider> instruments =  oracleProject.getInstruments().stream()
                .map(instrumet -> new DefaultCvParam(instrumet.getCvLabel(), instrumet.getAccession(), instrumet.getName(), instrumet.getValue()))
                .collect(Collectors.toList());

        //References
        List<ReferenceProvider> references = oracleProject.getReferences().stream()
                .map( reference -> new DefaultReference(reference.getReferenceLine(), reference.getPubmedId(), reference.getDoi()))
                .collect(Collectors.toList());

        //Modifications
        List<CvParamProvider> ptms = oracleProject.getPtms().stream().map(ptm -> new DefaultCvParam(ptm.getCvLabel(), ptm.getAccession(), ptm.getName(), ptm.getValue()))
                .collect(Collectors.toList());


        //Get software information
        List<CvParamProvider> softwareList = oracleProject.getSoftware()
                .stream()
                .filter(software -> software.getCvParam() != null )
                .map(software -> new DefaultCvParam(software.getCvParam().getCvLabel(), software.getCvParam().getAccession(),
                        software.getCvParam().getName(), software.getCvParam().getValue()))
                .collect(Collectors.toList());

        // Project Tags
        List<String> projectTags = oracleProject.getProjectTags().stream()
                .map(ProjectTag::getTag)
                .map(StringUtils::convertSentenceStyle)
                .collect(Collectors.toList());

        // Project Keywords
        List<String> keywords = Arrays.asList(oracleProject.getKeywords().split(",")).stream()
                .map(StringUtils::convertSentenceStyle).collect(Collectors.toList());

        //Project Quant Methods
        List<CvParamProvider> quantMethods = oracleProject.getQuantificationMethods().stream()
                .filter(quant -> quant.getCvParam() != null)
                .map(quant -> new DefaultCvParam(quant.getCvParam().getCvLabel(), quant.getCvParam().getAccession(),
                        quant.getCvParam().getName(), quant.getCvParam().getValue()))
                .collect(Collectors.toList());

        MongoPrideProject mongoProject = MongoPrideProject.builder()
                .title(oracleProject.getTitle())
                .accession(oracleProject.getAccession())
                .description(oracleProject.getProjectDescription())
                .sampleProcessing(oracleProject.getSampleProcessingProtocol())
                .dataProcessing(oracleProject.getDataProcessingProtocol())
                .keywords(keywords)
                .submissionDate(oracleProject.getSubmissionDate())
                .updatedDate(oracleProject.getUpdateDate())
                .publicationDate(oracleProject.getPublicationDate())
                .headLab(labHead)
                .submitters(submitters)
                .instruments(instruments)
                .references(references)
                .ptmList(ptms)
                .softwareList(softwareList)
                .projectTags(projectTags)
                .quantificationMethods(quantMethods)
                .build();
        return mongoProject;
    }
}
