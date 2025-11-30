package com.allendowney.thinkdast;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder; // Keep URLDecoder, though it's now unnecessary
import redis.clients.jedis.Jedis;

public class JedisMaker {

    /**
     * Make a Jedis object and authenticate it.
     *
     * @return
     * @throws IOException
     */
    public static Jedis make() throws IOException {
        
        // assemble the directory name
        String slash = File.separator;
        String filename = "resources" + slash + "redis_url.txt";
        
        // --- START MODIFIED SECTION ---
        // Change from classpath resource loading (which failed with NPE) to direct File loading
        File file = new File(filename);

        // open the file
        StringBuilder sb = new StringBuilder();
        BufferedReader br;
        try {
            // Use the File object directly
            br = new BufferedReader(new FileReader(file)); 
        } catch (FileNotFoundException e1) {
            System.out.println("File not found: " + filename);
            printInstructions();
            return null;
        }
        // --- END MODIFIED SECTION ---

        // read the file
        while (true) {
            String line = br.readLine();
            if (line == null) break;
            sb.append(line);
        }
        br.close();

        // parse the URL
        URI uri;
        try {
            uri = new URI(sb.toString());
        } catch (URISyntaxException e) {
            System.out.println("Reading file: " + filename);
            System.out.println("It looks like this file does not contain a valid URI.");
            printInstructions();
            return null;
        }
        String host = uri.getHost();
        int port = uri.getPort();

        // Note: The split logic assumes the format redis://AUTH@HOST:PORT or redis://redistogo:AUTH@HOST:PORT
        String[] array = uri.getAuthority().split("[:@]");
        String auth = array[1];
        
        // connect to the server
        Jedis jedis = new Jedis(host, port);

        try {
            jedis.auth(auth);
            return jedis;
        } catch (Exception e) {
            System.out.println("Trying to connect to " + host);
            System.out.println("on port " + port);
            System.out.println("with authcode " + auth);
            System.out.println("Got exception " + e);
            printInstructions();
            return null;
        }
    }


    /**
     * Print instructions for creating the config file.
     */
    private static void printInstructions() {
        System.out.println("");
        System.out.println("To connect to RedisToGo, you have to provide a file called");
        System.out.println("redis_url.txt that contains the URL of your Redis server.");
        System.out.println("If you select an instance on the RedisToGo web page,");
        System.out.println("you should see a URL that contains the information you need:");
        System.out.println("redis://redistogo:AUTH@HOST:PORT");
        System.out.println("Create a file called redis_url.txt in the src/resources");
        System.out.println("directory, and paste in the URL.");
    }


    /**
     * Example of using the Jedis connection.
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {

        Jedis jedis = make();
        
        // CRITICAL FIX: Check if make() failed and returned null
        if (jedis == null) {
            System.out.println("Jedis connection failed. Aborting main execution.");
            return;
        }
        
        // String
        jedis.set("mykey", "myvalue");
        String value = jedis.get("mykey");
        System.out.println("Got value: " + value);

        // Set
        jedis.sadd("myset", "element1", "element2", "element3");
        System.out.println("element2 is member: " + jedis.sismember("myset", "element2"));

        // List
        jedis.rpush("mylist", "element1", "element2", "element3");
        // Used int literal for index, which is often safer
        System.out.println("element at index 1: " + jedis.lindex("mylist", 1)); 

        // Hash
        jedis.hset("myhash", "word1", Integer.toString(2));
        jedis.hincrBy("myhash", "word2", 1);
        System.out.println("frequency of word1: " + jedis.hget("myhash", "word1"));
        System.out.println("frequency of word2: " + jedis.hget("myhash", "word2"));

        jedis.close();
    }
}