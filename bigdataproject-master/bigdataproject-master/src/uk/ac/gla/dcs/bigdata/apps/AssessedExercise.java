package uk.ac.gla.dcs.bigdata.apps;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.*;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticleEnricher;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsArticleToRankedResult;
import uk.ac.gla.dcs.bigdata.studentstructures.CorpusDetails;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleWithTerms;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator.similarity;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overridden by
 * the {@code spark.master} environment variable.
 * @author Richard
 *
 */
public class AssessedExercise {

	public static void main(String[] args) {
		
		File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file, so we can get an absolute path for it
		System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it
		
		// The code submitted for the assessed exercise may be run in either local or remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("spark.master");
		if (sparkMasterDef==null) sparkMasterDef = "local[4]"; // default is local mode with two executors
		
		String sparkSessionName = "BigDataAE"; // give the session a name
		
		// Create the Spark Configuration 
		SparkConf conf = new SparkConf()
				.setMaster(sparkMasterDef)
				.setAppName(sparkSessionName);
		
		// Create the spark session
		SparkSession spark = SparkSession
				  .builder()
				  .config(conf)
				  .getOrCreate();
	
		
		// Get the location of the input queries
		String queryFile = System.getenv("bigdata.queries");
		if (queryFile==null) queryFile = "data/queries.list"; // default is a sample with 3 queries
		
		// Get the location of the input news articles  TREC_Washington_Post_collection.v2.jl.fix.json
		String newsFile = System.getenv("bigdata.news");
		//if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news articles
		if (newsFile==null) newsFile = "data/TREC_Washington_Post_collection.v2.jl.fix.json";  //sample of 5gb data
		
		
		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);
		
		// Close the spark session
		spark.close();
		
		// Check if the code returned any results
		if (results==null) System.err.println("Topology return no rankings, student code may not be implemented, skipping final write.");
		else {
			
			// We have set of output rankings, lets write to disk
			
			// Create a new folder 
			File outDirectory = new File("results/"+System.currentTimeMillis());
			if (!outDirectory.exists()) System.out.println("mkdir output: " + outDirectory.mkdir());
			
			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(outDirectory.getAbsolutePath());
			}
		}

	}
	
	

	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {
		
		Dataset<Row> queriesJson = spark.read().text(queryFile);
		Dataset<Row> newsJson = spark.read().text(newsFile); // read in files as string rows, one row per article
		
		Dataset<Query> queries = queriesJson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); // this converts each row into a Query
		Dataset<NewsArticle> news = newsJson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle
		
		//----------------------------------------------------------------
		// Your Spark Topology should be defined here                   //
		//----------------------------------------------------------------

		
		/*
		 * implementing a search engine that will returns a ranked list of news articles based on a user's query
		 * queries dataset is a collection of user queries that will be used to rank the news articles
		 */
		
		Dataset<NewsArticleWithTerms> articles = news.map(new NewsArticleEnricher(), Encoders.bean(NewsArticleWithTerms.class));
		CorpusDetails corpus = getCorpusDetails(queries, articles);

		return queries.collectAsList()
				.stream()
				.map(query -> {
					List<RankedResult> rankedDocs = articles
							.filter((FilterFunction<NewsArticleWithTerms>) article -> article.getNewsArticle().getTitle() != null)
							.map(new NewsArticleToRankedResult(query, corpus), Encoders.bean(RankedResult.class))
							.filter((FilterFunction<RankedResult>) item -> !Double.isNaN(item.getScore()))
							.orderBy(functions.desc("score"))
							.collectAsList();

					return new DocumentRanking(query, removeNearDuplicates(rankedDocs));
				}).collect(Collectors.toList());
	}

	// this is method for scoring the relevance of each article to the user's query.
	private static CorpusDetails getCorpusDetails(Dataset<Query> queries, Dataset<NewsArticleWithTerms> articles) {

		Map<String, Long> allQueryTermsWithFreq = new HashMap<>();
     // flatmap methods will help to get a list of all unique query terms
		List<String> allArticleTerms = articles
				.flatMap((FlatMapFunction<NewsArticleWithTerms, String>) article -> article.getTerms().iterator(), Encoders.STRING())
				.collectAsList();

		queries
				.flatMap((FlatMapFunction<Query, String>) query -> query.getQueryTerms().iterator(), Encoders.STRING())
				.dropDuplicates()
				.collectAsList()
				.forEach(term -> allQueryTermsWithFreq.putIfAbsent(term, allArticleTerms
						.stream().filter(articleTerm -> articleTerm.equals(term)).count()));


		int totalDocLength = articles
				.map((MapFunction<NewsArticleWithTerms, Integer>) article -> article.getTerms().size(), Encoders.INT()) 
				.reduce((ReduceFunction<Integer>) Integer::sum); //contains the frequency of each query term in the corpus of news articles

		long totalDocs = articles.count();
		double avgDocLength = totalDocLength * 1.0 / totalDocs;

		return new CorpusDetails(allQueryTermsWithFreq, totalDocs, avgDocLength);
	}
	
	
	

	private static LinkedList<RankedResult> removeNearDuplicates(List<RankedResult> rankedDocs) {

		LinkedList<RankedResult> filteredRankedDocs = new LinkedList<>();

		for (RankedResult doc : rankedDocs) {
			if (filteredRankedDocs.size() == 10) break;

			if (filteredRankedDocs.isEmpty()) {
				filteredRankedDocs.add(doc);
			}

			RankedResult lastElement = filteredRankedDocs.getLast();
			if (similarity(doc.getArticle().getTitle(), lastElement.getArticle().getTitle()) >= 0.5) {
				filteredRankedDocs.add(doc);
			}
		}

		return filteredRankedDocs;
	}

}
