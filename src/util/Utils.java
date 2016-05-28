package AssociationRules.util;

// general dependencies
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Stack;
import java.util.Iterator;
import java.util.Comparator;
import java.lang.StringBuilder;

import java.io.OutputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.ByteArrayOutputStream;
import java.io.ByteArrayInputStream;
import org.apache.hadoop.fs.FileSystem;


public class Utils {

	// privately used static variables
    private static HashSet<List<Integer>> powerSet;
	private static List<Integer[]> permutationResult;



	public static String getVersion(){
		return "Association Rules: Version 0.0.1 ## Felix Last, Paul Englert, Lukas Fahr";
	}


   /*
   * sorts the map by values. Taken from:
   * http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html
   */
    public static <K extends Comparable, V extends Comparable> Map<K, V> sortMapByValues(Map<K, V> map) {
        List<Map.Entry<K, V>> entries = new LinkedList<Map.Entry<K, V>>(map.entrySet());

        Collections.sort(entries, new Comparator<Map.Entry<K, V>>() {

            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return o2.getValue().compareTo(o1.getValue());
            }
        });

        //LinkedHashMap will keep the keys in the order they are inserted
        //which is currently sorted on natural ordering
        Map<K, V> sortedMap = new LinkedHashMap<K, V>();

        for (Map.Entry<K, V> entry : entries) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }

    /*
    * returns all possible sortings of a given array
    * Source: http://stackoverflow.com/questions/14132877/order-array-in-every-possible-sequence
    *   returns an array set of Strings in all possible combinations
    */
    public static List<Integer[]> getAllPossiblePermutations(Integer[] inputArray){
        permutationResult = new ArrayList();

        // prepare input and start recursive generation
        Set<Integer> s = new HashSet(Arrays.asList(inputArray));
        permutations(s, new Stack<Integer>(), s.size());
        

        // copy set into temporary object
        List<Integer[]> out = new ArrayList(permutationResult);

        // clear powerSet to ensure integrity
        permutationResult = null;

        return out;     
    }

    private static void permutations(Set<Integer> items, Stack<Integer> permutation, int size) {

        /* permutation stack has become equal to size that we require */
        if(permutation.size() == size) {
            permutationResult.add(permutation.toArray(new Integer[0]));
        }

        /* items available for permutation */
        Integer[] availableItems = items.toArray(new Integer[0]);
        for(Integer i : availableItems) {
            /* add current item */
            permutation.push(i);

            /* remove item from available item set */
            items.remove(i);

            /* pass it on for next permutation */
            permutations(items, permutation, size);

            /* pop and put the removed item back */
            items.add(permutation.pop());
        }
    }

    /*
    *  generate all subsets of given length k from an inputset 
    *  Source: https://stackoverflow.com/questions/12548312/find-all-subsets-of-length-k-in-an-array
    */
    public static List<Set<Integer>> getSubsets(List<Integer> superSet, int k) {
        List<Set<Integer>> res = new ArrayList<>();
        getSubsets(superSet, k, 0, new HashSet<Integer>(), res);
        return res;
    }
    private static void getSubsets(List<Integer> superSet, int k, int idx, Set<Integer> current, List<Set<Integer>> solution) {
        //successful stop clause
        if (current.size() == k) {
            solution.add(new HashSet<>(current));
            return;
        }
        //unseccessful stop clause
        if (idx == superSet.size()) return;
        Integer x = superSet.get(idx);
        current.add(x);
        //"guess" x is in the subset
        getSubsets(superSet, k, idx+1, current, solution);
        current.remove(x);
        //"guess" x is not in the subset
        getSubsets(superSet, k, idx+1, current, solution);
    }


    /*
    *   returns a concatenation of an input array, with an optional delimiter
    */
    public static String concatenateArray(Integer[] input){
        return concatenateArray(input, "");
    }
    public static String concatenateArray(Integer[] input, String delimiter){
        String[] conv = new String[input.length];
        for (int i = 0; i < input.length; i++){
            conv[i] = ""+input[i];
        }
        return concatenateArray(conv,delimiter);
    }
    public static String concatenateArray(String[] input){
        return concatenateArray(input, "");
    }
    public static String concatenateArray(String[] input, String delimiter){
        String concatenated = "";
        for (int i = 0; i < input.length; i++){
            concatenated += input[i];
            if (i < input.length-1) concatenated += delimiter;
        }
        return concatenated;
    }


    /*
    *   (de-)serializes an object to specified path
    */
    public static Object deserializeObject(FileSystem fs, String pathStr) throws Exception{
        byte[] objectBytes = null;

        Path path = new Path(pathStr);
        InputStream in = fs.open(path);

        byte[] buffer = new byte[8192];
        int bytesRead;
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        while ((bytesRead = in.read(buffer)) != -1){
            output.write(buffer, 0, bytesRead);
        }
        objectBytes = output.toByteArray();

        return convertToObject(objectBytes);
    }

    public static void serializeObject(Object input, FileSystem fs, String pathStr) throws Exception{
        Path path = new Path(pathStr);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        OutputStream out = fs.create(path);
        out.write(convertToBytes(input));
        out.close();
    }

    public static void serializeHashMapReadable(Map<Integer, String> input, FileSystem fs, String pathStr) throws Exception{ 
        Path path = new Path(pathStr);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        FSDataOutputStream out = fs.create(path);

        boolean first = true;
        for (Integer key : input.keySet()) {
            if (first) {
                first = false;
            } else {
                out.writeBytes("\n");
            }
            out.writeBytes(""+key);
            out.writeBytes("\t");
            out.writeBytes(""+input.get(key));
        }
        out.close();
    }

    private static byte[] convertToBytes(Object input){
        ByteArrayOutputStream bos = null;
        ObjectOutput out = null;
        byte[] bytesArray = null;
        try {
            bos =  new ByteArrayOutputStream();
            out = new ObjectOutputStream(bos);   
            out.writeObject(input);
            bytesArray = bos.toByteArray();
        } catch(Exception e){
            System.err.println("Failed to create bytes array "+e.getMessage());
        }finally {
            try {
                if (out != null) {
                    out.close();
                }
            } catch (Exception ex) {
            // ignore close exception
            }
            try {
                bos.close();
            } catch (Exception ex) {
            // ignore close exception
            }
        }
        return bytesArray;
    }

    private static Object convertToObject(byte[] input){
        ByteArrayInputStream bis = null;
        ObjectInput in = null;
        Object o = null;
        try {
            bis = new ByteArrayInputStream(input);
            in = new ObjectInputStream(bis);
            o = in.readObject(); 
        } catch(Exception e){
            //
        }finally {
            try {
                bis.close();
            } catch (Exception ex) {
            // ignore close exception
            }
            try {
            if (in != null) {
                in.close();
            }
            } catch (Exception ex) {
            // ignore close exception
            }
        }
        return o;
    }

    /*
    *  creates a number from a string ("1;2;3;...") that is always the same for the same string (kind of a hashfunction)
    */
    public static Integer hashKey(String key){
        return hashKey(key, "addition");
    }
    public static Integer hashKey(String key, String type){
        // hash a key of integers to buckets (non-collision-free) trying to keep the size of the integer small 
        String[] parts = key.split(";");
        Integer result;
        
        switch (type){
            case "addition":
                result = 0;
                for (int i = 0; i < parts.length; i++){
                    result = result + Integer.parseInt(parts[i]);
                }
                break;
            case "stringutils":
                result = Math.round(key.hashCode()/20);
                result = (result >= 0) ? result : result*(-1);
                break;
            case "multiplication":
            default:
                result = 1;
                for (int i = 0; i < parts.length; i++){
                    result = result * Integer.parseInt(parts[i]);
                }
            break;
        }

        return  result;
    }
}
