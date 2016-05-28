package AssociationRules.mappers;

// package dependencies
import AssociationRules.util.Utils;

// hadoop dependencies
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

// general dependencies
import java.util.Set;
import java.util.HashSet;
import java.util.BitSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.fs.FileSystem;


/**
 * CandidateMapper is the mapper used for the frequent itemset extraction.
 * The class handles the extraction of itemsets with a fixed length from an input basket.<p>
 * It relies on the following job configurations:
 * <ul>
 * <li>TUPEL_SIZE: the size of the itemsets that are going to be extracted from a basket</li>
 * <li>TMP_FILE_PATH: the file system location, where the intermediary files are to be found</li>
 * <li>BASKET_ITEM_SPLITTER: the character sequence that separates items in the input basket</li>
 * </ul>
 *	<p>
 * The mapper will attempt to load the following files, but doesn't rely on them. The whitelist e.g. is not 
 * supposed to exist when the TUPEL_SIZE is equal 1.
 * <ul>
 * <li>item-keyMap.ser: the map serialization of the item names to integer key mapping</li>
 * <li>key-itemMap.ser: the map serialization of the integer keys to item name mapping</li>
 * <li>whitelist_x_tupel.ser: x is the current TUPEL_SIZE-1, the file contains the BitSet
 * representation of the previous frequent itemsets</li>
 * </ul>
 *
 * @author      Lukas Fahr
 * @author      Felix Last
 * @author      Paul Englert
 * @version     1.0 - 28.05.2016
 * @since       1.0
 */
public class CandidateMapper extends Mapper<Object, Text, Text, IntWritable> {

	/**
     * Enumerator keeping count of the following:
     * <ul>
     * <li>Inputlines: count of processed baskets</li>
     * <li>Items: number of distinct items found.</li>
     * <li>Writtensets: all sets that have been written to the context</li>
     * <li>Rejected_wl: all sets that have been rejected due to the whitelist</li>
     * <li>Rejected_size: all sets that have been rejected, because their size was smaller than the current TUPEL_SIZE</li>
     * </ul>
	 */
	public static enum Counters{
		WRITTENSETS,
		REJECTED_WL,
		REJECTED_SIZE,
		INPUTLINES,
		ITEMS
	}

	/**
     * Fixed output value.
     */
	private final static IntWritable one = new IntWritable(1);

	/**
     * Translate item name to integer value, loaded from file if possible.
     */
	private static Map<String, Integer> itemKey = new HashMap<>();

	/**
     * Translate integer value to item name, loaded from file if possible.
     */
	private static Map<Integer, String> keyItem = new HashMap<>();

	/**
     * Whitelist: BitSet to which frequent itemsets have been hashed during the previous iteration, loaded from file if possible.
     */
	private static BitSet whitelist = new BitSet();

	/**
     * Flag indicating whether a whitelist exists or not.
     */
	private static boolean noWhitelist = false;
	
	/**
     * Tupel size of current iteration. Updated from configuration.
     */
	private static int tupelSize = 0;

	
	/**
     * Character sequence splitting the input baskets into items. Updated from configuration.
     */
	private static String splitter = "\\.";

	/**
     * Path to intermediary files. Updated from configuration.
     */
	private static String basePath = "";


    /**
     * Preparing the mapper for execution by loading necessary files and reading configuration.
     * The mapping from item to keys and vice versa is loaded here, as well as the whitelist from
     * the previous iteration. If a file is missing, the members will be filled during execution.
     *
     * @param context   			context of mapper
     * 
     * @throws IOException			is called in a file system context
     * @throws InterruptedException	executed as thread, therefore can be interrupted
     *
     * @see 		AssociationRules.util.Utils#deserializeObject(FileSystem, String)
     *
     */
	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		
		// read configuration 

		basePath = context.getConfiguration().get("TMP_FILE_PATH");
		tupelSize = Integer.parseInt(context.getConfiguration().get("TUPEL_SIZE"));
		splitter = context.getConfiguration().get("BASKET_ITEM_SPLITTER");
		
		// try loading of helper files, such as
		// 		-	mapping of items to keys
		// 		-	whitelist of previous iteration 

		FileSystem fs = null;

		// try loading of mapping data
		try{
			fs = FileSystem.get(context.getConfiguration());
			itemKey = (HashMap) Utils.deserializeObject(fs, basePath+"item-keyMap.ser");
			keyItem = (HashMap) Utils.deserializeObject(fs, basePath+"key-itemMap.ser");
			System.out.println("Cached version of the items to key mapping found and loaded.");
		}catch(Exception e){
			itemKey = new HashMap<>();
			keyItem = new HashMap<>();
			System.out.println("No cached version of the items to key mapping found.");
		}

		// try loading of whitelist
		try{
			fs = FileSystem.get(context.getConfiguration());
			whitelist = (BitSet) Utils.deserializeObject(fs, basePath+"whitelist_"+(tupelSize-1)+"_tupel.ser");
			System.out.println("Whitelist file found from previous "+(tupelSize-1)+"-Tupel extraction and loaded with cardinality "+whitelist.cardinality()+".");
		} catch(Exception e){
			System.out.println("No Whitelist file found from previous "+(tupelSize-1)+"-Tupel extraction.");
			noWhitelist = true;
			whitelist = new BitSet();
		}
	}


	/**
     * Parses an input basket into the distinct items, translates them to integer keys and 
     * writes a count of one for each subset of a given size. The mapper will take an input basket (the value)
     * and split it at the specified delimiter into distinct items. If the number of items in the
     * basket is greater or equal to the current tupel size, the mapper will translate all items
     * to integer values with mappings from the file or with a newly created mapping. The mapper will 
     * then generate all subsets of size equal to the current tupel size and write a count of one to the context.<p>
     * INPUT FORMAT<p>
	 * key	: lineidentifier<br>
	 * value: basket, e.g. A,B,C,D<p>
	 *
	 * OUTPUT FORMAT<p>
	 * key	: Itemset, e.g. 1;2;3<br>
	 * value: 1	(count is always 1 ;) )<p>
	 *
     * @param key					a input file lineidentifier
     * @param value					the concatenated itemset-count pair
     * @param context 				the context of the map reduce job
     *
     * @throws IOException			is called in a file system context
     * @throws InterruptedException	executed as thread, therefore can be interrupted
     *
     * @see 		AssociationRules.util.Utils#getSubsets(List, int)
     * @see 		AssociationRules.util.Utils#concatenateArray(Integer[], String)
     */ 
	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// increment input counter
		context.getCounter(Counters.INPUTLINES).increment(1);
		
		// convert input into array
		String[] raw = value.toString().split(splitter);

		// only continue if the current basket has a large enough size
		if (raw.length >= tupelSize){
			// convert item names into integers and keep mapping data in map
			Integer[] rawConverted = new Integer[raw.length];
			for (int i = 0; i < raw.length; i++){
				Integer id = 0;
				if (itemKey.containsKey(raw[i])){
					id = itemKey.get(raw[i]);
				} else {
					context.getCounter(Counters.ITEMS).increment(1);
					id = (int) context.getCounter(Counters.ITEMS).getValue();
					itemKey.put(raw[i], id);
					keyItem.put(id, raw[i]);
				}
				rawConverted[i] = id;
			}

			// create every possible subset (consisting of tupelSize)
			List<Set<Integer>> powerset = new ArrayList<>();
			if (rawConverted.length == tupelSize){
				// use easy way out to save computation effort
				powerset.add(new HashSet<Integer>(Arrays.asList(rawConverted)));
			} else {
				// get the subsets the complicated way
				powerset = Utils.getSubsets(Arrays.asList(rawConverted), tupelSize);
			}

			// write each concatenated set to context with counter 1 if allowed by whitelist
			Iterator<Set<Integer>> it = powerset.iterator();
			while (it.hasNext()){
				Set<Integer> subset = it.next();
				Integer[] subsetArray = subset.toArray(new Integer[subset.size()]);
				
				if (noWhitelist || isWhitelisted(subsetArray)){
					String result = Utils.concatenateArray(subsetArray, ";");
					context.write(new Text(result), one);
					context.getCounter(Counters.WRITTENSETS).increment(1);
				} else {
					context.getCounter(Counters.REJECTED_WL).increment(1);
				}
			}
		} else {
			context.getCounter(Counters.REJECTED_SIZE).increment(1);
		}
	}

    /**
     * Finishing the mappers execution by writing intermediary files (mappings). 
     * During the cleanup the mapper will write the item names to integer mappings as serialized objects, as
     * well as one readable version to the file system.
     *
     * @param context   			context of mapper
     *
     * @see 		AssociationRules.util.Utils#serializeObject(Object, FileSystem, String)
     * @see 		AssociationRules.util.Utils#serializeHashMapReadable(Map, FileSystem, String)
     *
     */
	@Override
	public void cleanup(Context context){
		// serialize key - item mapping
		try{
			FileSystem fs = FileSystem.get(context.getConfiguration());
				
			Utils.serializeObject(itemKey, fs, basePath+"item-keyMap.ser");
	        System.out.println("Serialized item->key mapping "+basePath+"item-keyMap.ser");

			Utils.serializeObject(keyItem, fs, basePath+"key-itemMap.ser");
	        System.out.println("Serialized key->item mapping "+basePath+"key-itemMap.ser");

			Utils.serializeHashMapReadable(keyItem, fs, basePath+"mappingKeysToItems.txt");
	        System.out.println("Outputted key->item mapping into readable format in "+basePath+"mappingKeysToItems.txt");

		} catch(Exception e){
			System.err.println("failed serialization of item key mapping: "+e.getMessage());
		}

		//  log counters
		System.out.println("Cleanup CandidateMapper: Writtensets Counter = " + context.getCounter(Counters.WRITTENSETS).getValue());
		System.out.println("Cleanup CandidateMapper: Inputlines Counter = " + context.getCounter(Counters.INPUTLINES).getValue());
		System.out.println("Cleanup CandidateMapper: Rejected_WL Counter = " + context.getCounter(Counters.REJECTED_WL).getValue());
		System.out.println("Cleanup CandidateMapper: Rejected_Size Counter = " + context.getCounter(Counters.REJECTED_SIZE).getValue());
	}

    /**
     * Check whether all subsets of size n-1 of the input array (size n) are whitelisted. Get all subsets of the input array
     * with size n-1, n being the size of the input array and check whether they are on the whitelist. If yes return <code>true</code>,
     * else return <code>false</code>.
     *
     * @param input 	input superset
     * @return 			true if whitelisted, false if not
     *
     * @see 		AssociationRules.util.Utils#getSubsets(List, int)
     * @see 		#isWhitelisted(Set)
     *
     */
	private boolean isWhitelisted(Integer[] input){
		// check if all subsets (size tupelSize-1) of set are on whitelist
		boolean isAllowed = true;
		List<Set<Integer>> confirmationSet = Utils.getSubsets(Arrays.asList(input), input.length-1);
		Iterator<Set<Integer>> confSetIterator = confirmationSet.iterator();
		while (confSetIterator.hasNext()){
			Set<Integer> toTest = confSetIterator.next();
			if (!isWhitelisted(toTest)) isAllowed = false;
		}
		return isAllowed;
	}


    /**
     * Check whether a specific set is on the whitelist. This is achieved by checking the bit at the position
     * of the sets hash-code.
     *
     * @param input 	input set to check if it is on the whitelist
     * @return 			true if on whitelist, false if not
     *
     * @see 		AssociationRules.util.Utils#concatenateArray(Integer[], String)
     * @see 		AssociationRules.util.Utils#hashKey(String)
     *
     */
	private boolean isWhitelisted(Set<Integer> input){
		String key = Utils.concatenateArray(input.toArray(new Integer[0]), ";");
		Integer hash = Utils.hashKey(key);
		boolean whitelisted = true;
		try{
			whitelisted = whitelist.get(hash);
		} catch(Exception e){
			// probably index out of bounds -> but thats okay, that just means that the key is not on the whitelist!
			whitelisted = false;
		}
		return whitelisted;
	}


}
