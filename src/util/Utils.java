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


/**
 * Utils is the utility class used in the application.
 * The class has methods for several shared tasks of the application:
 * <ul>
 * <li>Hashmap Sorting</li>
 * <li>Array Permutation</li>
 * <li>Subset Generation</li>
 * <li>Array Concatenation</li>
 * <li>Object (De-)Serialization</li>
 * <li>String/Integer[] Hashing</li>
 * </ul>
 *
 * @author      Lukas Fahr
 * @author      Felix Last
 * @author      Paul Englert
 * @version     1.0 - 28.05.2016
 * @since       1.0
 */
public class Utils {

	/**
     * HashSet used for the generation of powersets, or subsets
     */
    private static HashSet<List<Integer>> powerSet;

    /**
     * List used for the generation of set permutations
     */
    private static List<Integer[]> permutationResult;



    /**
     * Get the version of the application.
     * @return version string of the application
     */
	public static String getVersion(){
		return "Association Rules: Version 1.0 ## Felix Last, Paul Englert, Lukas Fahr";
	}


    /**
     * Sorts a Map by its values.
     * Taken from:
     * <a>http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html</a>
     *
     * @param map   input map that is supposed to be sorted
     * @param <K>     key type of the map
     * @param <V>     value type of the map
     * @return      sorted input map
     *
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

    /**
     * Create all possible sortings/permutations of an array.
     * Taken from:
     * <a>http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html</a>
     *
     * @param inputArray    input array of which all permutations are generated
     * @return              list of all permutations of the input array
     *
     * @see                 #permutations(Set, Stack, int)
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

    /**
     * Helper method for recursive generation of permutations.
     * Used by @link AssociationRules.util.Utils#getAllPossiblePermutations(Integer[])
     * Taken from:
     * <a>http://javarevisited.blogspot.it/2012/12/how-to-sort-hashmap-java-by-key-and-value.html</a>
     *
     * @param items         available items to add to current permutation
     * @param permutation  currently processed permutation
     * @param size          size of desired permutation
     *
     * @see                 #getAllPossiblePermutations(Integer[])
     *
     */
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


    /**
     * Generate all subsets of given length k from an input set.
     * Taken from:
     * <a>https://stackoverflow.com/questions/12548312/find-all-subsets-of-length-k-in-an-array</a>
     *
     * @param superSet      input set of which subsets will be computed
     * @param k             size of subsets to be generated
     * @return              list of all subsets of size k of the input set
     *
     *
     * @see                 #getSubsets(List, int, int, Set, List)
     */
    public static List<Set<Integer>> getSubsets(List<Integer> superSet, int k) {
        List<Set<Integer>> res = new ArrayList<>();
        getSubsets(superSet, k, 0, new HashSet<Integer>(), res);
        return res;
    }

    /**
     * Helper method for recursive generation of permutations.
     * Used by @link AssociationRules.util.Utils#getSubsets(List, int)
     * Taken from:
     * <a>https://stackoverflow.com/questions/12548312/find-all-subsets-of-length-k-in-an-array</a>
     *
     * @param superSet      available items to create the subsets from
     * @param k             size of the subsets to generate
     * @param idx           position in superSet
     * @param current       current subset that is being modified
     * @param solution      final list of subsets to which current is added when it reaches a size of k
     *
     * @see                 #getSubsets(List, int)
     *
     */
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


    /**
     * Create a concatenation of an array of integers.
     * Concatenates an array without the use of a delimiter,
     * it simply adds the elements of the array to a string.
     *
     * @param  input     input array to concatenate
     * @return           concatenated array (without delimiter)
     *
     * @see                 #concatenateArray(Integer[], String)
     * @see                 #concatenateArray(String[])
     * @see                 #concatenateArray(String[], String)
     *
     */
    public static String concatenateArray(Integer[] input){
        return concatenateArray(input, "");
    }

    /**
     * Create a concatenation of an array of integers.
     * Concatenates an array with the use of a delimiter.
     *
     * @param  input         input array to concatenate
     * @param  delimiter     delimiter to use during concatenation
     * @return               concatenated array
     *
     * @see                 #concatenateArray(Integer[])
     * @see                 #concatenateArray(String[])
     * @see                 #concatenateArray(String[], String)
     *
     */
    public static String concatenateArray(Integer[] input, String delimiter){
        String[] conv = new String[input.length];
        for (int i = 0; i < input.length; i++){
            conv[i] = ""+input[i];
        }
        return concatenateArray(conv,delimiter);
    }

    /**
     * Create a concatenation of an array of strings.
     * Concatenates an array without the use of a delimiter,
     * it simply adds the elements of the array to a string.
     *
     * @param  input         input array to concatenate
     * @return               concatenated array (without delimiter)
     *
     * @see                 #concatenateArray(Integer[])
     * @see                 #concatenateArray(Integer[], String)
     * @see                 #concatenateArray(String[], String)
     *
     */
    public static String concatenateArray(String[] input){
        return concatenateArray(input, "");
    }

    /**
     * Create a concatenation of an array of strings.
     * Concatenates an array with the use of a delimiter
     *
     * @param  input         input array to concatenate
     * @param  delimiter     delimiter to use
     * @return               concatenated array
     *
     * @see                 #concatenateArray(Integer[])
     * @see                 #concatenateArray(Integer[], String)
     * @see                 #concatenateArray(String[])
     *
     */
    public static String concatenateArray(String[] input, String delimiter){
        String concatenated = "";
        for (int i = 0; i < input.length; i++){
            concatenated += input[i];
            if (i < input.length-1) concatenated += delimiter;
        }
        return concatenated;
    }


    /**
     * Deserialize an object from the file system.
     * Retrieves a serialization of an object from the file system,
     * by reading a byte stream and converting it into a java object.
     *
     * @param  fs              file system to read from
     * @param  pathStr         location of the file on the file system
     * @return                 retrieved object
     * @throws Exception       accessing the file system can fail if e.g. file does not exist
     *
     * @see                 #serializeObject(Object, FileSystem, String)
     * @see                 #serializeHashMapReadable(Map, FileSystem, String)
     * @see                 #convertToObject(byte[])
     * @see                 #convertToBytes(Object)
     *
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

    /**
     * Serialize an object and save to the file system.
     * Creates a serialization of an object,
     * by creating a byte stream and writing it onto the file system.
     *
     * @param  input           object to be serialized
     * @param  fs              file system to write onto
     * @param  pathStr         location of the file on the file system
     *
     * @throws Exception       accessing the file system can fail
     *
     * @see                 #deserializeObject(FileSystem, String)
     * @see                 #serializeHashMapReadable(Map, FileSystem, String)
     * @see                 #convertToObject(byte[])
     * @see                 #convertToBytes(Object)
     *
     */
    public static void serializeObject(Object input, FileSystem fs, String pathStr) throws Exception{
        Path path = new Path(pathStr);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
        OutputStream out = fs.create(path);
        out.write(convertToBytes(input));
        out.close();
    }


    /**
     * Save a Map to the file system in a readable way.
     * Creates a txt file from a Map by converting it,
     * into a String representation.
     *
     * @param  input           Map to be saved
     * @param  fs              file system to write onto
     * @param  pathStr         location of the file on the file system
     *
     * @throws Exception       accessing the file system can fail
     *
     * @see                 #deserializeObject(FileSystem, String)
     * @see                 #serializeObject(Object, FileSystem, String)
     * @see                 #convertToObject(byte[])
     * @see                 #convertToBytes(Object)
     *
     */
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


    /**
     * Creates a byte array from an object.
     * Converts an object into a output stream and captures
     * the data into a byte[].
     *
     * @param  input           object to be converted
     * @return                 array of bytes representing the object
     *
     *
     * @see                 #deserializeObject(FileSystem, String)
     * @see                 #serializeObject(Object, FileSystem, String)
     * @see                 #convertToObject(byte[])
     *
     */
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

    /**
     * Creates an object from a byte array.
     * Converts a byte array into an input stream and captures
     * the data into a Object.
     *
     * @param  input           byte array to be converted
     * @return                 object created from byte array
     *
     *
     * @see                 #deserializeObject(FileSystem, String)
     * @see                 #serializeObject(Object, FileSystem, String)
     * @see                 #convertToBytes(Object)
     *
     */
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


    /**
     * Calculates a hash-code from an itemset.
     * For a ;-separated itemset a hash-code will be 
     * generated using the addition-type of the method @link AssociationRules.util.Utils#hashKey(String, String).
     *
     * @param  key             input string (expecting a ;-separated itemset)
     * @return                 hash code of input
     *
     *
     * @see                 #hashKey(String, String)
     *
     */
    public static Integer hashKey(String key){
        return hashKey(key, "addition");
    }

    /**
     * Calculates a hash-code from an itemset with the given hash-type.
     * For a ;-separated itemset a hash-code will be 
     * generated using the any of the three types:
     * <ul>
     * <li>addition: simply sum up the item ids</li>
     * <li>multiplication: simply multiply the item ids</li>
     * <li>stringutils: use modified @link java.lang.String#hashCode()</li>
     * </ul>
     *
     * @param  key             input string (expecting a ;-separated itemset)
     * @param  type            type of hashing: addition, multiplication or stringutils
     * @return                 hash code of input
     *
     *
     * @see                 #hashKey(String, String)
     *
     */
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
