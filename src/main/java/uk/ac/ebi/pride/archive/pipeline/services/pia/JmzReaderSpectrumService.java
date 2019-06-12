package uk.ac.ebi.pride.archive.pipeline.services.pia;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import lombok.extern.slf4j.Slf4j;
import uk.ac.ebi.pride.archive.pipeline.utility.SubmissionPipelineConstants;
import uk.ac.ebi.pride.tools.jmzreader.JMzReader;
import uk.ac.ebi.pride.tools.jmzreader.JMzReaderException;
import uk.ac.ebi.pride.tools.jmzreader.model.Spectrum;
import uk.ac.ebi.pride.tools.mgf_parser.MgfFile;
import uk.ac.ebi.pride.tools.pride_wrapper.PRIDEXmlWrapper;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class JmzReaderSpectrumService {

    /**
     * Map of all readers containing the spectra
     */
    Map<String, JMzReader> readers = new HashMap<>();

    String clientRegion = "*** Client region ***";
    String bucketName = "*** Bucket name ***";
    String key = "*** Object key ***";

    AmazonS3 s3Client;

    private JmzReaderSpectrumService(Map<String, SubmissionPipelineConstants.FileType> spectrumFileList) throws JMzReaderException {
        this.readers = new HashMap<>();
        for (Map.Entry entry : spectrumFileList.entrySet()) {
            String key = (String) entry.getKey();
            SubmissionPipelineConstants.FileType value = (SubmissionPipelineConstants.FileType) entry.getValue();
            if (value == SubmissionPipelineConstants.FileType.MGF) {
                this.readers.put(key, new MgfFile(new File(key), true));
            }
            if (value == SubmissionPipelineConstants.FileType.PRIDE) {
                this.readers.put(key, new PRIDEXmlWrapper(new File(key)));
            }
        }
        ;
        s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(clientRegion)
                .withCredentials(new ProfileCredentialsProvider())
                .build();
    }

    /**
     * Return an instance that allow to read the spectra from the original file.
     *
     * @param spectrumFileList
     * @return
     * @throws JMzReaderException
     */
    public static JmzReaderSpectrumService getInstance(Map<String, SubmissionPipelineConstants.FileType> spectrumFileList) throws JMzReaderException {
        return new JmzReaderSpectrumService(spectrumFileList);
    }

    public Spectrum getSpectrum(String filePath, String id) throws JMzReaderException {
        JMzReader reader = readers.get(filePath);
        return reader.getSpectrumById(id);
    }


//    public String readS3PSM() throws IOException {
//
//        S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;
//        try {
//            System.out.println("Downloading an object");
//            fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));
//            System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
//            System.out.println("Content: ");
//            displayTextInputStream(fullObject.getObjectContent());
//
//            // Get a range of bytes from an object and print the bytes.
//            GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, key)
//                .withRange(0,9);
//            objectPortion = s3Client.getObject(rangeObjectRequest);
//            System.out.println("Printing bytes retrieved.");
//            displayTextInputStream(objectPortion.getObjectContent());
//        } catch(SdkClientException e) {
//            e.printStackTrace();
//        } finally {
//            if(fullObject != null) {
//                fullObject.close();
//            }
//            if(objectPortion != null) {
//            objectPortion.close();
//            }
//            if(headerOverrideObject != null) {
//                headerOverrideObject.close();
//            }
//        }
//        return "";
//    }
//
//    private void displayTextInputStream(S3ObjectInputStream objectContent) {
//
//    }

}