package uk.ac.ebi.pride.archive.pipeline.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.pride.archive.pipeline.tasklets.PxXmlUpdater;
import uk.ac.ebi.pride.archive.px.xml.XMLParams;
import uk.ac.ebi.pride.archive.utils.config.FilePathBuilder;
import uk.ac.ebi.pride.archive.utils.config.FilePathBuilderPride3;
import uk.ac.ebi.pride.archive.utils.streaming.FileUtils;

@Configuration
public class ProteomeCentralConfig {


    @Value("${pride.data.prod.directory}")
    String prePublicDatasetDir;

    @Value("${px.partner.name}")
    String pxPartnerName;

    @Value("${px.partner.pass}")
    private String pxPassword;

    @Value("${px.partner.test}")
    private String pxTest;

    @Bean
    public FileUtils fileUtils() {
        return new FileUtils();
    }

    @Bean
    public XMLParams xmlParams() {
        XMLParams xmlParams = new XMLParams();
        xmlParams.setPxPartner(pxPartnerName);
        xmlParams.setAuthentication(pxPassword);
        xmlParams.setTest(pxTest);
        xmlParams.setMethod("submitDataset");
        xmlParams.setVerbose("no");
        return xmlParams;
    }

    @Bean
    public PxXmlUpdater pxXmlUpdater() {
        return new PxXmlUpdater(fileUtils(), filePathBuilder(), xmlParams(), prePublicDatasetDir);
    }

    @Bean
    public FilePathBuilder filePathBuilder() {
        return new FilePathBuilderPride3();
    }
}
