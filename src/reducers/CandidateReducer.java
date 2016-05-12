package AssociationRules.reducers;

// package dependencies
import AssociationRules.util.Utils;

// hadoop dependencies
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// general dependencies
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.HashMap;
import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.fs.FileSystem;


public class CandidateReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	/*
	*		INPUT FORMAT
	*						key		: Itemset <{itemset}>, e.g. 1;2;3
	*						value	: <frequency>, e.g. 4
	*
	*		OUTPUT FORMAT
	*						key		: frequent itemset <{itemset}>, e.g. 1;2;3
	*						value	: frequency of itemset <frequency>, e.g. 5 
	*
	*/

	public static enum Counters{
		FREQUENT_ITEMSETS,
		DECLINED_SETS
	}


	// translate text to integer id
	// public static Map<String, Integer> itemKey = new HashMap<>(); // not needed
	// public static Map<Integer, String> keyItem = new HashMap<>();

	// whitelist for SON approach
	public static Set<Integer> whitelist = new HashSet<>();

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// read the mapping of keys to item names
		// try{
		// 	String path = context.getConfiguration().get("TMP_FILE_PATH");
		// 	FileSystem fs = FileSystem.get(context.getConfiguration());
		// 	keyItem = (HashMap) Utils.deserializeObject(fs, path+"key-itemMap.ser");
		// 	System.out.println("Successfully deserialized item key mapping.");
		// } catch(Exception e){
		// 	System.err.println("Failed deserialization of item key mapping: "+e.getMessage());
		// }
	}

	// key is subset of a basket, values are the counts
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		//  sum up counts of input values
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		// get support threshold
		int supportThreshold = Integer.parseInt(context.getConfiguration().get("SUPPORT_THRESHOLD"));

		// write itemset out if frequent
		if (sum >= supportThreshold){
			// context.write(new Text(keyItem.get(Integer.parseInt(key.toString()))), new IntWritable(sum));
			for (String k : key.toString().split(";")){
				whitelist.add(Integer.parseInt(k));
			}
			context.write(new Text(key.toString()), new IntWritable(sum));
			context.getCounter(Counters.FREQUENT_ITEMSETS).increment(1);
		} else{
			context.getCounter(Counters.DECLINED_SETS).increment(1);
		}
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
	
		// write whitelist to fs for use in the next mapper iteration 
		String basePath = context.getConfiguration().get("TMP_FILE_PATH");
		Integer tupelSize = Integer.parseInt(context.getConfiguration().get("TUPEL_SIZE"));
		FileSystem fs = null;
		try{
			fs = FileSystem.get(context.getConfiguration());
			Utils.serializeObject(whitelist, fs, basePath+"whitelist_"+(tupelSize)+"_tupel.ser");
			System.out.println("Whitelist persisted for "+(tupelSize)+"-Tupel extraction.");
		} catch(Exception e){
			System.err.println("Whitelist not persisted for "+(tupelSize)+"-Tupel extraction.");
		}

		// serialize key - item mapping otherwise it's not available for the next job...
		// if (keyItem.size() > 0){
		// 	try{
		// 		fs = FileSystem.get(context.getConfiguration());
		// 		Utils.serializeObject(keyItem, fs, basePath+"key-itemMap.ser");
		//         System.out.println("Serialized key->item mapping "+basePath+"key-itemMap.ser");
		// 	} catch(Exception e){
		// 		System.err.println("failed serialization of item key mapping: "+e.getMessage());
		// 	}
		// }

		// log counters
		System.out.println("CandidateReducer accepted sets: " + context.getCounter(Counters.FREQUENT_ITEMSETS).getValue());
		System.out.println("CandidateReducer declined sets: " + context.getCounter(Counters.DECLINED_SETS).getValue());

	}
}
