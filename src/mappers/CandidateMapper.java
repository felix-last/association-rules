package AssociationRules.mappers;

// package dependencies
import AssociationRules.util.Utils;

// hadoop dependencies
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;

// general dependencies
import java.util.Set;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
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
	public static Map<String, Integer> itemKey = new HashMap<>();
	public static Map<Integer, String> keyItem = new HashMap<>();

	// whitelist for candidate set generation (SON Approach)
	public static Set<String> whitelist = new HashSet<>();
	public static boolean noWhitelist = false;

	// tupel size of curren iteration
	public static int numElements = 0;


	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		// try loading of helper files, such as
		// 		-	mapping of items to keys
		// 		-	whitelist of previous iteration 

		String basePath = context.getConfiguration().get("TMP_FILE_PATH");
		Integer tupelSize = Integer.parseInt(context.getConfiguration().get("TUPEL_SIZE"));
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
			whitelist = (HashSet) Utils.deserializeObject(fs, basePath+"whitelist_"+(tupelSize-1)+"_tupel.ser");
			System.out.println("Whitelist file found from previous "+(tupelSize-1)+"-Tupel extraction and loaded.");
		} catch(Exception e){
			System.out.println("No Whitelist file found from previous "+(tupelSize-1)+"-Tupel extraction.");
			noWhitelist = true;
			whitelist = new HashSet<String>();
		}

		// get tupel size of iteration
		numElements = Integer.parseInt(context.getConfiguration().get("TUPEL_SIZE"));
	}

	@Override
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// increment input counter
		context.getCounter(Counters.INPUTLINES).increment(1);
		
		// convert input into array
		String splitter = context.getConfiguration().get("BASKET_ITEM_SPLITTER");
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

		// check if all subsets (size tupelSize-1) of set is on whitelist
		boolean isAllowed = true;
		if (!noWhitelist){
			List<Set<Integer>> confirmationSet = Utils.getSubsets(Arrays.asList(rawConverted), numElements-1);
			Iterator<Set<Integer>> confSetIterator = confirmationSet.iterator();
			while (confSetIterator.hasNext()){
				if (!isWhitelisted(confSetIterator.next())) isAllowed = false;
			}
		}

		if (isAllowed){
			// convert to set (ensures that there are no duplicates)
			List<Integer> inputSet = Arrays.asList(rawConverted);

			// create every possible subset (consisting of numElements)
			List<Set<Integer>> powerset = Utils.getSubsets(inputSet, numElements);

			// write each concatenated set to context with counter 1
			Iterator<Set<Integer>> it = powerset.iterator();
			while (it.hasNext()){
				Set<Integer> subset = it.next();
				String result = Utils.concatenateArray(subset.toArray(new Integer[subset.size()]), ";");
				context.write(new Text(result), one);
				context.getCounter(Counters.WRITTENSETS).increment(1);
			}
		} else {
			context.getCounter(Counters.REJECTED_WL).increment(1);
		}
	}

	@Override
	public void cleanup(Context context){
		// serialize key - item mapping
		String path = context.getConfiguration().get("TMP_FILE_PATH");
		try{
			FileSystem fs = FileSystem.get(context.getConfiguration());
				
			Utils.serializeObject(itemKey, fs, path+"item-keyMap.ser");
	        System.out.println("Serialized item->key mapping "+path+"item-keyMap.ser");

			Utils.serializeObject(keyItem, fs, path+"key-itemMap.ser");
	        System.out.println("Serialized key->item mapping "+path+"key-itemMap.ser");

			Utils.serializeHashMapReadable(keyItem, fs, path+"mappingKeysToItems.txt");
	        System.out.println("Outputted key->item mapping into readable format in "+path+"mappingKeysToItems.txt");

		} catch(Exception e){
			System.err.println("failed serialization of item key mapping: "+e.getMessage());
		}

		//  log counters
		System.out.println("Cleanup CandidateMapper: Writtensets Counter = " + context.getCounter(Counters.WRITTENSETS).getValue());
		System.out.println("Cleanup CandidateMapper: Inputlines Counter = " + context.getCounter(Counters.INPUTLINES).getValue());
		System.out.println("Cleanup CandidateMapper: Rejected_WL Counter = " + context.getCounter(Counters.REJECTED_WL).getValue());
	}

	private boolean isWhitelisted(Set<Integer> input){
		boolean whitelisted = true;
		// prepare input key for comparison
		Integer[] comb =  input.toArray(new Integer[0]);
		Arrays.sort(comb);
		String k = Utils.concatenateArray(comb, ";");
		// check in whitelist
		if (!whitelist.contains(k)) whitelisted = false;
		return whitelisted;
	}

}
