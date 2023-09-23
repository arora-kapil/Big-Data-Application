package uk.ac.gla.dcs.bigdata.studentstructures;

import uk.ac.gla.dcs.bigdata.providedstructures.ContentItem;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/*
 * NewsArticleWithTerms class holds a NewsArticle object and a list of terms extracted from the article's title and contents.
 * This class is processing the article's title and the first five paragraphs of type "paragraph" using a TextPreProcessor object.
 */

public class NewsArticleWithTerms implements Serializable {

    private final NewsArticle newsArticle;

    private final List<String> terms;

    public NewsArticleWithTerms(NewsArticle newsArticle) {
        this.newsArticle = newsArticle;

        TextPreProcessor textPreProcessor = new TextPreProcessor();
        List<String> strings = new ArrayList<>();

        strings.add(newsArticle.getTitle());
        strings.addAll(newsArticle
                .getContents().stream()
                .filter(contentItem -> contentItem !=null && contentItem.getSubtype() != null && contentItem.getSubtype().equals("paragraph"))
                .limit(5)
                .map(ContentItem::getContent)
                .collect(Collectors.toList()));

        //The resulting list of terms will be stored in the terms field.
        terms = strings.stream()
                .flatMap(str -> textPreProcessor.process(str).stream())
                .collect(Collectors.toList());
    }

    public NewsArticle getNewsArticle() {
        return newsArticle;
    }

    public List<String> getTerms() {
        return terms;
    }

}

