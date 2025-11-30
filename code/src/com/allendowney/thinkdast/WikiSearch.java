package com.allendowney.thinkdast;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;

import redis.clients.jedis.Jedis;

/**
 * Represents the results of a search query.
 */
public class WikiSearch {

    // map from URLs that contain the term(s) to relevance score
    private Map<String, Integer> map;

    /**
     * Constructor.
     *
     * @param map
     */
    public WikiSearch(Map<String, Integer> map) {
        // CRITICAL FIX: Ensure this.map is never null.
        this.map = (map == null) ? new HashMap<String, Integer>() : map;
    }

    /**
     * Looks up the relevance of a given URL.
     *
     * @param url
     * @return
     */
    public Integer getRelevance(String url) {
        Integer relevance = map.get(url);
        return relevance == null ? 0 : relevance;
    }

    /**
     * Prints the contents in order of term frequency.
     *
     * @param
     */
    private void print() {
        List<Entry<String, Integer>> entries = sort();

        if (entries.isEmpty()) {
            System.out.println("(No results found.)");
            return;
        }
        
        for (Entry<String, Integer> entry : entries) {
            System.out.println(entry.getKey() + " (" + entry.getValue() + ")");
        }
    }

    /**
     * Computes the union of two search results (OR operation).
     *
     * @param that
     * @return New WikiSearch object.
     */
    public WikiSearch or(WikiSearch that) {
        Map<String, Integer> result = new HashMap<String, Integer>(this.map);

        for (Entry<String, Integer> entry : that.map.entrySet()) {
            String url = entry.getKey();
            Integer thatRelevance = entry.getValue();
            
            if (result.containsKey(url)) {
                Integer thisRelevance = result.get(url);
                Integer totalRel = this.totalRelevance(thisRelevance, thatRelevance);
                result.put(url, totalRel);
            } else {
                result.put(url, thatRelevance);
            }
        }
        return new WikiSearch(result);
    }

    /**
     * Computes the intersection of two search results (AND operation).
     *
     * @param that
     * @return New WikiSearch object.
     */
    public WikiSearch and(WikiSearch that) {
        Map<String, Integer> result = new HashMap<String, Integer>();

        for (Entry<String, Integer> entry : this.map.entrySet()) {
            String url = entry.getKey();
            
            if (that.map.containsKey(url)) {
                
                Integer thisRelevance = entry.getValue();
                Integer thatRelevance = that.getRelevance(url);
                
                Integer totalRel = this.totalRelevance(thisRelevance, thatRelevance);
                
                result.put(url, totalRel);
            }
        }
        return new WikiSearch(result);
    }

    /**
     * Computes the difference of two search results (MINUS operation).
     *
     * @param that
     * @return New WikiSearch object.
     */
    public WikiSearch minus(WikiSearch that) {
        Map<String, Integer> result = new HashMap<String, Integer>(this.map);

        for (String url : that.map.keySet()) {
            result.remove(url);
        }
        return new WikiSearch(result);
    }

    /**
     * Computes the relevance of a search with multiple terms (sum of frequencies).
     *
     * @param rel1: relevance score for the first search
     * @param rel2: relevance score for the second search
     * @return
     */
    protected int totalRelevance(Integer rel1, Integer rel2) {
        return rel1 + rel2;
    }

    /**
     * Sort the results by relevance in increasing order (lowest score first).
     *
     * @return List of entries with URL and relevance.
     */
    public List<Entry<String, Integer>> sort() {
        
        if (map.isEmpty()) {
            return new ArrayList<>(); 
        }
        
        List<Entry<String, Integer>> entries = new LinkedList<Entry<String, Integer>>(map.entrySet());

        Comparator<Entry<String, Integer>> comparator = new Comparator<Entry<String, Integer>>() {
            @Override
            public int compare(Entry<String, Integer> entry1, Entry<String, Integer> entry2) {
                return entry1.getValue() - entry2.getValue(); 
            }
        };

        Collections.sort(entries, comparator); 

        return entries;
    }


    /**
     * Performs a search and makes a WikiSearch object.
     *
     * @param term
     * @param index
     * @return
     */
    public static WikiSearch search(String term, JedisIndex index) {
        Map<String, Integer> map = index.getCounts(term);
        return new WikiSearch(map);
    }

    public static void main(String[] args) throws IOException {

        // --- START HARDCODED TEST DATA TO JUSTIFY LOGIC ---
        
        Map<String, Integer> map1 = new HashMap<String, Integer>();
        map1.put("Page1", 1);
        map1.put("Page2", 2);
        map1.put("Page3", 3);
        WikiSearch search1 = new WikiSearch(map1);

        Map<String, Integer> map2 = new HashMap<String, Integer>();
        map2.put("Page2", 4);
        map2.put("Page3", 5);
        map2.put("Page4", 7);
        WikiSearch search2 = new WikiSearch(map2);
        
        System.out.println("\n--- FINAL LOGIC VERIFICATION WITH HARDCODED DATA ---");

        // Test 1: Search 1 (Ascending Sort)
        String term1 = "TEST 1 (Page1:1, Page2:2, Page3:3)";
        System.out.println("\nQuery: " + term1);
        search1.print();

        // Test 2: Search 2 (Ascending Sort)
        String term2 = "TEST 2 (Page2:4, Page3:5, Page4:7)";
        System.out.println("\nQuery: " + term2);
        search2.print();
        
        // Test 3: AND (Intersection) -> Expected Pages: Page2 (6), Page3 (8). Sorted: Page2, Page3
        System.out.println("\nQuery: TEST 1 AND TEST 2");
        WikiSearch intersection = search1.and(search2);
        intersection.print();
        
        // Test 4: OR (Union) -> Expected Pages: Page1 (1), Page2 (6), Page3 (8), Page4 (7). Sorted: Page1, Page2, Page4, Page3
        System.out.println("\nQuery: TEST 1 OR TEST 2");
        WikiSearch union = search1.or(search2);
        union.print();

        // Test 5: MINUS (Difference) -> Expected Pages: Page1 (1). Sorted: Page1
        System.out.println("\nQuery: TEST 1 MINUS TEST 2");
        WikiSearch difference = search1.minus(search2);
        difference.print();
        
        // --- END HARDCODED TEST DATA ---
    }
}