
package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleWithTerms;

/*
This implements the MapFunction interface with a NewsArticle input and a NewsArticleWithTerms output. 
The call method returns a new NewsArticleWithTerms object initialized with the input NewsArticle.

*/

public class NewsArticleEnricher implements MapFunction<NewsArticle, NewsArticleWithTerms> {

    @Override
    public NewsArticleWithTerms call(NewsArticle newsArticle) throws Exception {
        return new NewsArticleWithTerms(newsArticle);
    }

}
