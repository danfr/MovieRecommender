package com.camillepradel.movierecommender.controller;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.neo4j.driver.v1.*;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

@Controller
public class MainController {
	String message = "Welcome to Spring MVC!";
 
	@RequestMapping("/hello")
	public ModelAndView showMessage(
			@RequestParam(value = "name", required = false, defaultValue = "World") String name) {
		System.out.println("in controller");
 
		ModelAndView mv = new ModelAndView("helloworld");
		mv.addObject("message", message);
		mv.addObject("name", name);
		return mv;
	}

	@RequestMapping("/movies")
	public ModelAndView showMovies(
			@RequestParam(value = "user_id", required = false) Integer userId) {
		System.out.println("show Movies of user " + userId);

        // return showMoviesMongo(userId); // Requetes vers mongoDB
        return showMoviesNeo(userId); // Requetes vers Neo4j
    }

    private ModelAndView showMoviesMongo(@RequestParam(value = "user_id", required = false) Integer userId) {
        MongoClient mongo = MongoConnect();

        List<Movie> movies;

        if (userId != null && userId != 0)
            movies = getMoviesMDB(mongo, userId);
        else
            movies = getMoviesMDB(mongo);

        ModelAndView mv = new ModelAndView("movies");
        mv.addObject("userId", userId);
        mv.addObject("movies", movies);

        MongoStop(mongo);

        return mv;
    }

    private ModelAndView showMoviesNeo(@RequestParam(value = "user_id", required = false) Integer userId) {
        Driver neo = NeoConnect();

        List<Movie> movies;

        if (userId != null && userId != 0)
            movies = getMoviesNeo(neo, userId);
        else
            movies = getMoviesNeo(neo);

        ModelAndView mv = new ModelAndView("movies");
        mv.addObject("userId", userId);
        mv.addObject("movies", movies);

        NeoStop(neo);

        return mv;
    }

    private List<Movie> getMoviesNeo(Driver neo, Integer userId) {
        return null;
    }

    private List<Movie> getMoviesNeo(Driver neo) {
        ArrayList<Movie> movies = new ArrayList<Movie>();

        Session session = neo.session();

        StatementResult result = session.run("MATCH (m:Movie)-[:CATEGORIZED_AS]->(g:Genre)\n" +
                "RETURN m.id AS id, m.title AS title, g.name AS genre\n" +
                "ORDER BY id");

        while (result.hasNext()) {
            Record record = result.next();
            movies.add(getMovie(record));
        }

        session.close();

        return movies;
    }

    private Driver NeoConnect() {
        Driver driver = null;
        try {
            driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "neo4j"));
        } catch (Exception e) {
            System.err.println("Erreur de connexion à la base Neo4j :");
            e.printStackTrace();
        }

        Session session = driver.session();

        StatementResult result = session.run("MATCH (a:User) RETURN a.id AS id");
        if (!result.hasNext())
            System.err.println("La base MovieLens n'existe pas dans Neo4j");

        session.close();

        return driver;
    }

    private void NeoStop(Driver neo) {
        neo.close();
    }

    private List<Movie> getMoviesMDB(MongoClient mongo) {
        List<Movie> movies = new LinkedList<Movie>();

        MongoDatabase db = mongo.getDatabase("MovieLens");
        MongoCollection table = db.getCollection("movies");

        BasicDBObject searchQuery = new BasicDBObject();

        FindIterable cursor = table.find(searchQuery);

        for (Object obj : cursor) {
            Document doc = (Document) obj;

            Movie mov = getMovie(doc);
            movies.add(mov);
        }

        return movies;
    }

    private Movie getMovie(Document doc) {
        String tousgenres = doc.getString("genres");
        String[] listegenre = tousgenres.split("\\|");
        ArrayList<Genre> genres = new ArrayList<Genre>();

        for (String g : listegenre) {
            genres.add(new Genre(0, g));
        }

        return new Movie(doc.getInteger("_id"), doc.getString("title"), genres);
    }

    private Movie getMovie(Record rec) {
        List<Object> listegenre = rec.get("genre").asList();
        ArrayList<Genre> genres = new ArrayList<Genre>();

        for (Object g : listegenre) {
            String str = (String) g;
            genres.add(new Genre(0, str));
        }

        return new Movie(rec.get("id").asInt(), rec.get("title").asString(), genres);
    }

    private List<Movie> getMoviesMDB(MongoClient mongo, Integer userId) {
        List<Movie> movies = new LinkedList<Movie>();

        MongoDatabase db = mongo.getDatabase("MovieLens");
        MongoCollection table = db.getCollection("users");
        MongoCollection table2 = db.getCollection("movies");

        BasicDBObject searchQuery = new BasicDBObject();
        searchQuery.put("_id", userId);

        FindIterable cursor = table.find(searchQuery);

        for (Object obj : cursor) {
            Document user = (Document) obj;
            ArrayList<Document> list_mov = (ArrayList) user.get("movies");
            for (Document doc : list_mov) {
                Integer mov_id = doc.getInteger("movieid");
                BasicDBObject searchQuery2 = new BasicDBObject();
                searchQuery2.put("_id", mov_id);
                FindIterable cursor2 = table2.find(searchQuery2);
                Document mov = (Document) cursor2.first();
                Movie m = getMovie(mov);
                movies.add(m);
            }
        }

        return movies;
    }

    private MongoClient MongoConnect() {
        MongoClient mongo = null;

        try {
            mongo = new MongoClient("localhost", 27017);
        } catch (Exception e) {
            System.err.println("Erreur de connexion à MongoDB :");
            e.printStackTrace();
        }

        try {
            MongoDatabase db = mongo.getDatabase("MovieLens");
            System.out.println("Connection à la base MongoDB '" + db.getName() + "' effectuée");
        } catch (Exception e) {
            System.err.println("La base MovieLens n'existe pas :");
            e.printStackTrace();
        }

        return mongo;
    }

    private void MongoStop(MongoClient mongo) {
        mongo.close();
    }

    @RequestMapping(value = "/movieratings", method = RequestMethod.GET)
    public ModelAndView showMoviesRattings(
            @RequestParam(value = "user_id", required = true) Integer userId) {
        System.out.println("GET /movieratings for user " + userId);

        // TODO: write query to retrieve all movies from DB
        List<Movie> allMovies = new LinkedList<Movie>();
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        allMovies.add(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})));
        allMovies.add(new Movie(1, "Titre 1", Arrays.asList(new Genre[]{genre0, genre2})));
        allMovies.add(new Movie(2, "Titre 2", Arrays.asList(new Genre[]{genre1})));
        allMovies.add(new Movie(3, "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})));

        // TODO: write query to retrieve all ratings from the specified user
        List<Rating> ratings = new LinkedList<Rating>();
        ratings.add(new Rating(new Movie(0, "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 3));
        ratings.add(new Rating(new Movie(2, "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));

        ModelAndView mv = new ModelAndView("movieratings");
        mv.addObject("userId", userId);
        mv.addObject("allMovies", allMovies);
        mv.addObject("ratings", ratings);

        return mv;
    }

    @RequestMapping(value = "/movieratings", method = RequestMethod.POST)
    public String saveOrUpdateRating(@ModelAttribute("rating") Rating rating) {
        System.out.println("POST /movieratings for user " + rating.getUserId()
                + ", movie " + rating.getMovie().getId()
                + ", score " + rating.getScore());

        // TODO: add query which
        //         - add rating between specified user and movie if it doesn't exist
        //         - update it if it does exist

        return "redirect:/movieratings?user_id=" + rating.getUserId();
    }

    @RequestMapping(value = "/recommendations", method = RequestMethod.GET)
    public ModelAndView ProcessRecommendations(
            @RequestParam(value = "user_id", required = true) Integer userId,
            @RequestParam(value = "processing_mode", required = false, defaultValue = "0") Integer processingMode) {
        System.out.println("GET /movieratings for user " + userId);

        // TODO: process recommendations for specified user exploiting other users ratings
        //       use different methods depending on processingMode parameter
        Genre genre0 = new Genre(0, "genre0");
        Genre genre1 = new Genre(1, "genre1");
        Genre genre2 = new Genre(2, "genre2");
        List<Rating> recommendations = new LinkedList<Rating>();
        String titlePrefix;
        if (processingMode.equals(0))
            titlePrefix = "0_";
        else if (processingMode.equals(1))
            titlePrefix = "1_";
        else if (processingMode.equals(2))
            titlePrefix = "2_";
        else
            titlePrefix = "default_";
        recommendations.add(new Rating(new Movie(0, titlePrefix + "Titre 0", Arrays.asList(new Genre[]{genre0, genre1})), userId, 5));
        recommendations.add(new Rating(new Movie(1, titlePrefix + "Titre 1", Arrays.asList(new Genre[]{genre0, genre2})), userId, 5));
        recommendations.add(new Rating(new Movie(2, titlePrefix + "Titre 2", Arrays.asList(new Genre[]{genre1})), userId, 4));
        recommendations.add(new Rating(new Movie(3, titlePrefix + "Titre 3", Arrays.asList(new Genre[]{genre0, genre1, genre2})), userId, 3));

        ModelAndView mv = new ModelAndView("recommendations");
        mv.addObject("recommendations", recommendations);

        return mv;
    }

}
