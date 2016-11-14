package com.camillepradel.movierecommender.controller;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

import java.util.ArrayList;
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

		MongoClient mongo = MongoConnect();

		List<Movie> movies;

		if (userId != null && userId != 0)
			movies = getMoviesMDB(mongo, userId);
		else
			movies = getMoviesMDB(mongo);

		/*
		List<Movie> movies = new LinkedList<Movie>();
		Genre genre0 = new Genre(0, "genre0");
		Genre genre1 = new Genre(1, "genre1");
		Genre genre2 = new Genre(2, "genre2");
		movies.add(new Movie(0, "Titre 0", Arrays.asList(new Genre[] {genre0, genre1})));
		movies.add(new Movie(1, "Titre 1", Arrays.asList(new Genre[] {genre0, genre2})));
		movies.add(new Movie(2, "Titre 2", Arrays.asList(new Genre[] {genre1})));
		movies.add(new Movie(3, "Titre 3", Arrays.asList(new Genre[] {genre0, genre1, genre2})));
		*/

		ModelAndView mv = new ModelAndView("movies");
		mv.addObject("userId", userId);
		mv.addObject("movies", movies);

		MongoStop(mongo);

		return mv;
	}

	private List<Movie> getMoviesMDB(MongoClient mongo) {
		List<Movie> movies = new LinkedList<Movie>();

		MongoDatabase db = mongo.getDatabase("MovieLens");
		MongoCollection table = db.getCollection("movies");

		BasicDBObject searchQuery = new BasicDBObject();

		FindIterable cursor = table.find(searchQuery);

		for (Object obj : cursor) {
			Document doc = (Document) obj;

			String tousgenres = doc.getString("genres");
			String[] listegenre = tousgenres.split("\\|");
			ArrayList<Genre> genres = new ArrayList<Genre>();

			for (String g : listegenre) {
				genres.add(new Genre(0, g));
			}

			Movie mov = new Movie(doc.getInteger("_id"), doc.getString("title"), genres);
			movies.add(mov);
		}

		return movies;
	}

	private List<Movie> getMoviesMDB(MongoClient mongo, Integer userId) {
		List<Movie> movies = new LinkedList<Movie>();

		MongoDatabase db = mongo.getDatabase("MovieLens");
		MongoCollection table = db.getCollection("users");

		BasicDBObject searchQuery = new BasicDBObject();
		searchQuery.put("_id", userId);

		FindIterable cursor = table.find(searchQuery);

		for (Object obj : cursor) {
			Document doc = (Document) obj;
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
}
