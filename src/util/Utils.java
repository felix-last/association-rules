package AssociationRules.util;

// general dependencies
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.HashSet;
import java.util.List;
import java.util.LinkedList;
import java.util.ArrayList;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Stack;
import java.util.Iterator;
import java.util.Comparator;


public class Utils {

	// privately used static variables
    private static HashSet<List<String>> powerSet;
	private static List<String[]> permutationResult;



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
    public static List<String[]> getAllPossiblePermutations(String[] inputArray){
        permutationResult = new ArrayList();

        // prepare input and start recursive generation
        Set<String> s = new HashSet(Arrays.asList(inputArray));
        permutations(s, new Stack<String>(), s.size());
        

        // copy set into temporary object
        List<String[]> out = new ArrayList(permutationResult);

        // clear powerSet to ensure integrity
        permutationResult = null;

        return out;     
    }

    private static void permutations(Set<String> items, Stack<String> permutation, int size) {

        /* permutation stack has become equal to size that we require */
        if(permutation.size() == size) {
            /* print the permutation */
            // System.out.println(Arrays.toString(permutation.toArray(new String[0])));
            permutationResult.add(permutation.toArray(new String[0]));
        }

        /* items available for permutation */
        String[] availableItems = items.toArray(new String[0]);
        for(String i : availableItems) {
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
    *   returns all possible subsets of a given input set
    * Source: http://stackoverflow.com/questions/18800850/finding-all-subsets-of-a-set-powerset
    */
	public static HashSet<List<String>> powerSet(Set<String> inputSet){
		return powerSet(inputSet, 0);
	}
	public static HashSet<List<String>> powerSet(Set<String> inputSet, int minSubSetSize){

		// generate power set
		List<String> mainList = new ArrayList<String>(inputSet);
		powerSet = new HashSet();
    	buildPowerSet(mainList,mainList.size());

    	// remove all subsets with less items than minSubSetSize
    	// TODO can this be included into the buildPowerSet()? would be more efficient than going through the set again
    	Iterator<List<String>> it = powerSet.iterator();
    	while (it.hasNext()){
    		if (it.next().size() < minSubSetSize){
    			it.remove();
    		}
    	}

    	// copy power set into temporary object
    	HashSet<List<String>> out = new HashSet();
    	out.addAll(powerSet);

    	// clear powerSet to ensure integrity
    	powerSet = null;

	    return out;
	}


    /*
    * Source: http://stackoverflow.com/questions/18800850/finding-all-subsets-of-a-set-powerset
    */
	private static void buildPowerSet(List<String> list, int count){
		powerSet.add(list);

	    for(int i=0; i<list.size(); i++){
	        List<String> temp = new ArrayList<String>(list);
	        temp.remove(i);
	        buildPowerSet(temp, temp.size());
		}
	}

    /*
    *   returns a concatenation of an input array, with an optional delimiter
    */
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
}
