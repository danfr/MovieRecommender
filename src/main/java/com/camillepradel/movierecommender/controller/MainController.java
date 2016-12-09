package com.camillepradel.movierecommender.controller;

import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.MongoClient;
import org.neo4j.driver.v1.Driver;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.servlet.ModelAndView;

import java.util.ArrayList;
import java.util.List;

@Controller
public class MainController {
    private String message = "Welcome to Spring MVC!";
    private static final Boolean MONGO = true; // Changer cette variable pour switcher d'une base à l'autre
 
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

        if (MONGO)
            return showMoviesMongo(userId); // Requetes vers mongoDB
        else
            return showMoviesNeo(userId); // Requetes vers Neo4j
    }

    private ModelAndView showMoviesMongo(@RequestParam(value = "user_id", required = false) Integer userId) {
        MongoClient mongo = Delegate.MongoConnect();

        List<Movie> movies;

        if (userId != null && userId != 0)
            movies = Delegate.getMoviesMDB(mongo, userId);
        else
            movies = Delegate.getMoviesMDB(mongo);

        ModelAndView mv = new ModelAndView("movies");
        mv.addObject("userId", userId);
        mv.addObject("movies", movies);

        Delegate.MongoStop(mongo);

        return mv;
    }

    private ModelAndView showMoviesNeo(@RequestParam(value = "user_id", required = false) Integer userId) {
        Driver neo = Delegate.NeoConnect();

        List<Movie> movies;

        if (userId != null && userId != 0)
            movies = Delegate.getMoviesNeo(neo, userId);
        else
            movies = Delegate.getMoviesNeo(neo);

        ModelAndView mv = new ModelAndView("movies");
        mv.addObject("userId", userId);
        mv.addObject("movies", movies);

        Delegate.NeoStop(neo);

        return mv;
    }

    @RequestMapping(value = "/movieratings", method = RequestMethod.GET)
    public ModelAndView showMoviesRatings(
            @RequestParam(value = "user_id", required = true) Integer userId) {
        System.out.println("GET /movieratings for user " + userId);

        if (MONGO)
            return showRatingsMongo(userId); // Requetes vers mongoDB
        else
            return showRatingsNeo(userId); // Requetes vers Neo4j
    }

    private ModelAndView showRatingsMongo(Integer userId) {
        MongoClient mongo = Delegate.MongoConnect();

        List<Movie> movies = new ArrayList<Movie>();
        List<Rating> ratings = new ArrayList<Rating>();

        if (userId != null && userId != 0) {
            movies = Delegate.getMoviesMDB(mongo);
            ratings = Delegate.getRatingsMDB(mongo, userId);
        }

        ModelAndView mv = new ModelAndView("movieratings");
        mv.addObject("userId", userId);
        mv.addObject("allMovies", movies);
        mv.addObject("ratings", ratings);

        Delegate.MongoStop(mongo);

        return mv;
    }

    private ModelAndView showRatingsNeo(Integer userId) {
        Driver neo = Delegate.NeoConnect();

        List<Movie> movies = new ArrayList<Movie>();
        List<Rating> ratings = new ArrayList<Rating>();

        if (userId != null && userId != 0) {
            movies = Delegate.getMoviesNeo(neo);
            ratings = Delegate.getRatingsNeo(neo, userId);
        }

        ModelAndView mv = new ModelAndView("movieratings");
        mv.addObject("userId", userId);
        mv.addObject("allMovies", movies);
        mv.addObject("ratings", ratings);

        Delegate.NeoStop(neo);

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

        if (MONGO)
            Delegate.setRatingMDB(rating); // Requetes vers mongoDB
        else
            Delegate.setRatingNeo(rating); // Requetes vers mongoDB

        return "redirect:/movieratings?user_id=" + rating.getUserId();
    }

    @RequestMapping(value = "/recommendations", method = RequestMethod.GET)
    public ModelAndView ProcessRecommendations(
            @RequestParam(value = "user_id", required = true) Integer userId,
            @RequestParam(value = "processing_mode", required = false, defaultValue = "0") Integer processingMode) {
        System.out.println("GET /movieratings for user " + userId);

        ArrayList<Rating> recommendations = new ArrayList<Rating>();

        switch (processingMode) {
            case 1:
                recommendations = (MONGO) ? Delegate.recoMDB(userId, 1) : Delegate.recoNeo1(userId);
                break;
            case 2:
                recommendations = (MONGO) ? Delegate.recoMDB(userId, 5) : Delegate.recoNeo2(userId);
                break;
            case 3:
                recommendations = (MONGO) ? Delegate.recoMDB(userId, 5) : Delegate.recoNeo2(userId);
                break;
        }

        ModelAndView mv = new ModelAndView("recommendations");
        mv.addObject("recommendations", recommendations);

        return mv;
    }

}
