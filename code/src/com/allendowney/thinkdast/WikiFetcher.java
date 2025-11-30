package com.allendowney.thinkdast;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.jsoup.Connection;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;


public class WikiFetcher {
    private long lastRequestTime = -1;
    private long minInterval = 1000;
    
    // Define a standard User-Agent string to avoid 403 Forbidden errors
    private static final String USER_AGENT = 
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36";

    /**
     * Fetches and parses a URL string, returning a list of paragraph elements.
     *
     * @param url
     * @return
     * @throws IOException
     */
    public Elements fetchWikipedia(String url) throws IOException {
        sleepIfNeeded();

        // download and parse the document
        Connection conn = Jsoup.connect(url)
                               .userAgent(USER_AGENT); // <-- FIX APPLIED HERE
                               
        Document doc = conn.get();

        // select the content text and pull out the paragraphs.
        Element content = doc.getElementById("mw-content-text");

        // TODO: avoid selecting paragraphs from sidebars and boxouts
        Elements paras = content.select("p");
        return paras;
    }

    /**
     * Reads the contents of a Wikipedia page from src/resources.
     *
     * @param url
     * @return
     * @throws IOException
     */
    public Elements readWikipedia(String url) throws IOException {
        URL realURL = new URL(url);

        // assemble the file name
        String slash = File.separator;
        String filename = "resources" + slash + realURL.getHost() + realURL.getPath();

        // read the file
        InputStream stream = WikiFetcher.class.getClassLoader().getResourceAsStream(filename);
        Document doc = Jsoup.parse(stream, "UTF-8", filename);

        // parse the contents of the file
        Element content = doc.getElementById("mw-content-text");
        Elements paras = content.select("p");
        return paras;
    }

    /**
     * Rate limits by waiting at least the minimum interval between requests.
     */
    private void sleepIfNeeded() {
        if (lastRequestTime != -1) {
            long currentTime = System.currentTimeMillis();
            long nextRequestTime = lastRequestTime + minInterval;
            if (currentTime < nextRequestTime) {
                try {
                    //System.out.println("Sleeping until " + nextRequestTime);
                    Thread.sleep(nextRequestTime - currentTime);
                } catch (InterruptedException e) {
                    System.err.println("Warning: sleep interrupted in fetchWikipedia.");
                }
            }
        }
        lastRequestTime = System.currentTimeMillis();
    }

    /**
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        WikiFetcher wf = new WikiFetcher();
        String url = "https://en.wikipedia.org/wiki/Java_(programming_language)";
        
        // Changed to use fetchWikipedia to test the live connection and fix
        Elements paragraphs = wf.fetchWikipedia(url); 

        for (Element paragraph: paragraphs) {
            System.out.println(paragraph);
        }
    }
}