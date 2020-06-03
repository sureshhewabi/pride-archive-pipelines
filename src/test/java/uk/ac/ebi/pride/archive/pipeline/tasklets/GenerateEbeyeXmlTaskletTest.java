package uk.ac.ebi.pride.archive.pipeline.tasklets;


import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.util.Map;
import java.util.Set;

@RunWith(SpringJUnit4ClassRunner.class)
@TestPropertySource(properties = {"backupPath=src/test/resources/"})
public class GenerateEbeyeXmlTaskletTest {

    private GenerateEbeyeXmlTasklet generateEbeyeXmlTasklet;

    @Before
    public void before() {
        generateEbeyeXmlTasklet = new GenerateEbeyeXmlTasklet();
        generateEbeyeXmlTasklet.setBackupPath("src/test/resources/");
    }

    @Test
    public void getProteinMappingTest() throws Exception {
        Map<String, String> proteinMapping = generateEbeyeXmlTasklet.restoreFromFile("PXD002633");
        Assert.assertNotNull(proteinMapping.size());
        Assert.assertEquals(1, proteinMapping.size());
        Assert.assertTrue(proteinMapping.containsKey("APOB_HUMAN"));
        Assert.assertEquals("uniprot", proteinMapping.get("APOB_HUMAN"));

    }

}
