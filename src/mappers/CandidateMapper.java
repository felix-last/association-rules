package AssociationRules.mappers;

// package dependencies
import AssociationRules.util.Utils;

// hadoop dependencies
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

// general dependencies
import java.util.Set;
import java.util.BitSet;
import java.util.Map;
import java.util.HashMap;
import java.util.Arrays;
import java.util.List;
import java.util.Iterator;
import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.fs.FileSystem;


public class CandidateMapper extends Mapper<Object, Text, Text, IntWritable> {

	/*
	*		INPUT FORMAT
	*						key		: lineidentifier
	*						value	: basket <{basket items}>, e.g. A,B,C,D
	*
	*		OUTPUT FORMAT
	*						key		: Itemset <{itemset converted to integer keys}>, e.g. 1;2;3
	*						value	: 1	(count is always 1 ;) ) 
	*
	*/

	public static enum Counters{
		WRITTENSETS,
		REJECTED_WL,
		INPUTLINES,
		ITEMS
	}

	private final static IntWritable one = new IntWritable(1);

	// translate text to integer id
	private static Map<String, Integer> itemKey = new HashMap<>();
	private static Map<Integer, String> keyItem = new HashMap<>();

	// whitelist for candidate set generation (SON Approach)
	private static BitSet whitelist = new BitSet();
	private static boolean noWhitelist = false;

	// tupel size of current iteration
	private static int tupelSize = 0;

	// input file splitter
	private static String splitter = "\\.";

	// helperfiles location
	private static String basePath = "";


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

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// increment input counter
		context.getCounter(Counters.INPUTLINES).increment(1);
		
		// convert input into array
		String[] raw = value.toString().split(splitter);

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
		List<Set<Integer>> powerset = Utils.getSubsets(Arrays.asList(rawConverted), tupelSize);

		// write each concatenated set to context with counter 1 if allowed by whitelist
		Iterator<Set<Integer>> it = powerset.iterator();
		while (it.hasNext()){
			Set<Integer> subset = it.next();
			Integer[] subsetArray = subset.toArray(new Integer[subset.size()]);
			
			// check if all subsets (size tupelSize-1) of set are on whitelist
			boolean isAllowed = true;
			if (!noWhitelist){
				List<Set<Integer>> confirmationSet = Utils.getSubsets(Arrays.asList(subsetArray), tupelSize-1);
				Iterator<Set<Integer>> confSetIterator = confirmationSet.iterator();
				while (confSetIterator.hasNext()){
					Set<Integer> toTest = confSetIterator.next();
					if (!isWhitelisted(toTest)) isAllowed = false;
				}
			}

			if (isAllowed){
				String result = Utils.concatenateArray(subsetArray, ";");
				context.write(new Text(result), one);
				context.getCounter(Counters.WRITTENSETS).increment(1);
			} else {
				context.getCounter(Counters.REJECTED_WL).increment(1);
			}
		}
	}

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
	}

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
