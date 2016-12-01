package com.camillepradel.movierecommender.controller;

import com.camillepradel.movierecommender.model.Genre;
import com.camillepradel.movierecommender.model.Movie;
import com.camillepradel.movierecommender.model.Rating;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.neo4j.driver.v1.*;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;

public class Delegate {
    static List<Movie> getMoviesNeo(Driver neo, Integer userId) {
        ArrayList<Movie> movies = new ArrayList<Movie>();

        Session session = neo.session();

        StatementResult result = session.run("MATCH (u:User{id:" + userId + "})-[:RATED]->(m:Movie)-[:CATEGORIZED_AS]->(g:Genre)\n" +
                "RETURN m.id AS id, m.title AS title, collect(g.name) AS genre\n" +
                "ORDER BY id");

        while (result.hasNext()) {
            Record record = result.next();
            movies.add(getMovie(record));
        }

        session.close();

        return movies;
    }

    static List<Movie> getMoviesNeo(Driver neo) {
        ArrayList<Movie> movies = new ArrayList<Movie>();

        Session session = neo.session();

        StatementResult result = session.run("MATCH (m:Movie)-[:CATEGORIZED_AS]->(g:Genre)\n" +
                "RETURN m.id AS id, m.title AS title, collect(g.name) AS genre\n" +
                "ORDER BY id");

        while (result.hasNext()) {
            Record record = result.next();
            movies.add(getMovie(record));
        }

        session.close();

        return movies;
    }

    static Driver NeoConnect() {
        Driver driver = null;
        try {
            driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "123"));
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

    static void NeoStop(Driver neo) {
        neo.close();
    }

    static List<Movie> getMoviesMDB(MongoClient mongo) {
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

    static Movie getMovie(Document doc) {
        String tousgenres = doc.getString("genres");
        String[] listegenre = tousgenres.split("\\|");
        ArrayList<Genre> genres = new ArrayList<Genre>();

        for (String g : listegenre) {
            genres.add(new Genre(0, g));
        }

        return new Movie(doc.getInteger("_id"), doc.getString("title"), genres);
    }

    static Movie getMovie(Record rec) {
        List<Object> listegenre = rec.get("genre").asList();
        ArrayList<Genre> genres = new ArrayList<Genre>();

        for (Object g : listegenre) {
            String str = (String) g;
            genres.add(new Genre(0, str));
        }

        return new Movie(rec.get("id").asInt(), rec.get("title").asString(), genres);
    }

    static List<Movie> getMoviesMDB(MongoClient mongo, Integer userId) {
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

    static MongoClient MongoConnect() {
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

    static void MongoStop(MongoClient mongo) {
        mongo.close();
    }

    public static List<Rating> getRatingsNeo(Driver neo, Integer userId) {
        ArrayList<Rating> movies = new ArrayList<Rating>();

        Session session = neo.session();

        StatementResult result = session.run("MATCH (u:User{id:" + userId + "})-[r:RATED]->(m:Movie)-[:CATEGORIZED_AS]->(g:Genre) RETURN m.id AS id, u.id AS user, m.title AS title, collect(g.name) AS genre, r.note AS note ORDER BY id");

        while (result.hasNext()) {
            Record record = result.next();
            movies.add(getRating(record));
        }

        session.close();

        return movies;
    }

    private static Rating getRating(Record rec) {
        List<Object> listegenre = rec.get("genre").asList();
        ArrayList<Genre> genres = new ArrayList<Genre>();

        for (Object g : listegenre) {
            String str = (String) g;
            genres.add(new Genre(0, str));
        }

        Movie mov = new Movie(rec.get("id").asInt(), rec.get("title").asString(), genres);
        return new Rating(mov, rec.get("user").asInt(), rec.get("note").asInt());
    }

    public static List<Rating> getRatingsMDB(MongoClient mongo, Integer userId) {
        List<Rating> movies = new ArrayList<Rating>();

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
                int note = doc.getInteger("rating");
                BasicDBObject searchQuery2 = new BasicDBObject();
                searchQuery2.put("_id", mov_id);
                FindIterable cursor2 = table2.find(searchQuery2);
                Document mov = (Document) cursor2.first();
                Movie m = getMovie(mov);
                movies.add(new Rating(m, userId, note));
            }
        }

        return movies;
    }

    static void setRatingMDB(Rating rating) {
        MongoClient mongo = Delegate.MongoConnect();
        Bson filter = Filters.eq("_id", rating.getUserId());

        int time = (int) (new Date().getTime() / 1000);

        Bson update = new Document("$pull",
                new Document("movies",
                        new Document("movieid", rating.getMovieId())
                ));

        mongo.getDatabase("MovieLens")
                .getCollection("users")
                .updateOne(filter, update);

        update = new Document("$push",
                new Document("movies",
                        new Document()
                                .append("movieid", rating.getMovieId())
                                .append("rating", rating.getScore())
                                .append("timestamp", time)
                ));

        mongo.getDatabase("MovieLens")
                .getCollection("users")
                .updateOne(filter, update);

        Delegate.MongoStop(mongo);
    }

    public static void setRatingNeo(Rating rating) {
        Driver neo = Delegate.NeoConnect();

        Session session = neo.session();
        int time = (int) (new Date().getTime() / 1000);

        session.run("MATCH (u:User{id:" + rating.getUserId() + "})-[rel:RATED]->(m:Movie{id:" + rating.getMovieId() + "})\n" +
                "DELETE rel");

        session.run("MATCH (u:User {id:" + rating.getUserId() + "}), (m:Movie {id:" + rating.getMovieId() + "})\n" +
                "CREATE (u)-[:RATED{note:" + rating.getScore() + ", timestamp:" + time + "}]->(m)");

        session.close();
        Delegate.NeoStop(neo);
    }

    public static ArrayList<Rating> recoNeo1(Integer userId) {
        ArrayList<Rating> result = new ArrayList<Rating>();

        Driver neo = Delegate.NeoConnect();
        Session session = neo.session();

        StatementResult res = session.run("MATCH (target_user:User {id : " + userId + "})-[:RATED]->(m:Movie)<-[:RATED]-(other_user:User)\n" +
                "WITH other_user, count(distinct m.title) AS num_common_movies, target_user\n" +
                "ORDER BY num_common_movies DESC\n" +
                "LIMIT 1\n" +
                "MATCH (other_user)-[rat_other_user:RATED]->(m2:Movie)\n" +
                "WHERE NOT ((target_user)-[:RATED]->(m2))\n" +
                "RETURN m2.id AS mov_id, m2.title AS rec_movie_title, rat_other_user.note AS rating, other_user.id AS watched_by\n" +
                "ORDER BY rat_other_user.note DESC");

        while (res.hasNext()) {
            Record record = res.next();
            result.add(new Rating(new Movie(record.get("mov_id").asInt(), record.get("rec_movie_title").asString(), null), userId, record.get("rating").asInt()));
        }

        session.close();
        Delegate.NeoStop(neo);

        return result;
    }

    public static ArrayList<Rating> recoMDB1(Integer userId) {
        return null;
    }

    public static ArrayList<Rating> recoMDB2(Integer userId) {
        return null;
    }

    public static ArrayList<Rating> recoNeo2(Integer userId) {
        ArrayList<Rating> result = new ArrayList<Rating>();

        Driver neo = Delegate.NeoConnect();
        Session session = neo.session();

        StatementResult res = session.run("MATCH (target_user:User {id : " + userId + "})-[:RATED]->(m:Movie)<-[:RATED]-(other_user:User) \n" +
                "WITH other_user, count(distinct m.title) AS num_common_movies, target_user \n" +
                "ORDER BY num_common_movies DESC \n" +
                "LIMIT 5\n" +
                "MATCH (other_user)-[rat_other_user:RATED]->(m2:Movie) \n" +
                "WHERE NOT ((target_user)-[:RATED]->(m2)) \n" +
                "RETURN m2.id AS mov_id, m2.title AS rec_movie_title, AVG(rat_other_user.note) AS rating, COUNT(other_user.id)\n" +
                "ORDER BY AVG(rat_other_user.note) DESC, COUNT(other_user.id) DESC");

        while (res.hasNext()) {
            Record record = res.next();
            double moyenne = record.get("rating").asDouble();
            int rating = (int) moyenne;
            result.add(new Rating(new Movie(record.get("mov_id").asInt(), record.get("rec_movie_title").asString(), null), userId, rating));
        }

        session.close();
        Delegate.NeoStop(neo);

        return result;
    }
}