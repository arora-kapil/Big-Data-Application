
package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.Map;

//This class contain information of corpus details(all articles)

public class CorpusDetails implements Serializable {

    private final Map<String, Long> allQueryTermsWithFreq;
    private final long totalDocs;
    private final double avgDocLength;

    public CorpusDetails(Map<String, Long> allQueryTermsWithFreq, long totalDocs, double avgDocLength) {
        this.allQueryTermsWithFreq = allQueryTermsWithFreq; // a map of all the query terms and their frequency in the corpus
        this.totalDocs = totalDocs; //the total number of documents 
        this.avgDocLength = avgDocLength; //the average length of a document
    }

    /*
     * We have used three getter methods to access these fields. 
     * Also, implementing the Serializable interface which support serialization.
     */
    public int getTotalDocs() {
        return (int) totalDocs;
    }

    public double getAvgDocLength() {
        return avgDocLength;
    }

    public Map<String, Long> getAllQueryTermsWithFreq() {
        return allQueryTermsWithFreq;
    }

}
