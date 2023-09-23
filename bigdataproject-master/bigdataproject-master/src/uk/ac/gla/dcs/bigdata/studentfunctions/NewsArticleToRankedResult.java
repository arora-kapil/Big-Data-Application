
package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.CorpusDetails;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleWithTerms;

import java.util.List;
import java.util.stream.Collectors;


/*
 * this class implements the MapFunction interface to convert a NewsArticleWithTerms object to a RankedResult object. 
 * The constructor takes a Query and CorpusDetails object. 
 */
public class NewsArticleToRankedResult implements MapFunction<NewsArticleWithTerms, RankedResult> {

    private final Query query;
    private final CorpusDetails corpus;

    public NewsArticleToRankedResult(Query query, CorpusDetails corpus) {
        this.query = query;
        this.corpus = corpus;
    }

    /*
     * The call() method calculates the DPH scores for each term in the Query against the NewsArticleWithTerms object and returns
     
     *     a RankedResult object with the article's id, article object, and query level score.
     */

    @Override
    public RankedResult call(NewsArticleWithTerms article) throws Exception {

        List<Double> scores = query.getQueryTerms()
                .stream()
                .map(term -> {
                    short termFreqInCurrentDoc = (short) article.getTerms()
                            .stream()
                            .filter(aTerm -> aTerm.equals(term))
                            .count();

                    return DPHScorer.getDPHScore(
                            termFreqInCurrentDoc,
                            corpus.getAllQueryTermsWithFreq().get(term).intValue(),
                            article.getTerms().size(),
                            corpus.getAvgDocLength(),
                            corpus.getTotalDocs());
                })
                .collect(Collectors.toList());

 //calculating the total sum of a list of double values called scores using the reduce method and stores the result in the total
        double total = scores.stream().reduce(0D, Double::sum);

        double queryLevelScore = total / query.getQueryTerms().size(); //calculating the average score per query term by dividing

        return new RankedResult(article.getNewsArticle().getId(), article.getNewsArticle(), queryLevelScore); //will return information about a news article's relevance to a particular search query
    }

}
