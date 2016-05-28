package AssociationRules.reducers;

// package dependencies
import AssociationRules.util.Utils;

// hadoop dependencies
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// general dependencies
import java.util.BitSet;
import java.io.IOException;
import java.lang.InterruptedException;
import org.apache.hadoop.fs.FileSystem;

/**
 * CandidateReducer will sum up the counts of itemsets and pass them on if they are frequent.
 * The reducer will only pass on itemsets whose count exceeds a support threshold. Those itemsets
 * will also be put onto a BitSet whitelist, that is being persisted for use in the next iteration.<p>
* It relies on the following job configurations:
 * <ul>
 * <li>SUPPORT_THRESHOLD: support threshold to decide whether itemset is frequent or not</li>
 * <li>TMP_FILE_PATH: the file system location, where the intermediary files are to be found</li>
 * <li>TUPEL_SIZE: tuple size of the current iteration</li>
 * </ul>
 *	<p>
 *
 * @author      Lukas Fahr
 * @author      Felix Last
 * @author      Paul Englert
 * @version     1.0 - 28.05.2016
 * @since       1.0
 */
public class CandidateReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	/**
     * Enumerator keeping count of the following:
     * <ul>
     * <li>frequent_itemsets: count of accepted frequent itemsets</li>
     * <li>declined_sets: count of declined, non-frequent itemsets.</li>
     * </ul>
	 */
	public static enum Counters{
		FREQUENT_ITEMSETS,
		DECLINED_SETS
	}

	/**
     * Whitelist: BitSet to which frequent itemsets will be hashed.
     */
	private static BitSet whitelist = new BitSet();
	
	/**
     * Support threshold that decides whether itemsets are frequent or not. Updated from configuration.
     */
	private static int supportThreshold = 0;

	/**
     * Location of intermediary files. Updated from configuration.
     */
	private static String basePath = "";

	/**
     * Tuple size of current iteration. Updated from configuration.
     */
	private static Integer tupelSize = 0;



    /**
     * Preparing the reducer by loading the configuration for the support threshold, the intermediary file location
     * and the tuple size of the current iteration.
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
	protected void setup(Context context) throws IOException, InterruptedException {
		// read configuration
		supportThreshold = Integer.parseInt(context.getConfiguration().get("SUPPORT_THRESHOLD"));
		basePath = context.getConfiguration().get("TMP_FILE_PATH");
		tupelSize = Integer.parseInt(context.getConfiguration().get("TUPEL_SIZE"));
	}


	/**
     * Reduce function sums up the counts for itemsets and passes them on if they exceed the support threshold.
     * If an itemset exceeds the threshold it is also hashed to the whitelist which will be persisted later on.<p>
     * INPUT FORMAT<p>
	 * key	: Itemset, e.g. 1;2;3<br>
	 * values: 2342, 34, 32,... <p>
	 *
	 * OUTPUT FORMAT<p>
	 * key	: Itemset, e.g. 1;2;3<br>
	 * value: 4332 <p>
	 *
     * @param key					itemset key, e.g. 1;2;3
     * @param values				the iterable counts for the key
     * @param context 				the context of the map reduce job
     *
     * @throws IOException			is called in a file system context
     * @throws InterruptedException	executed as thread, therefore can be interrupted
     */ 
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		//  sum up counts of input values
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}


		// write itemset out if frequent
		if (sum >= supportThreshold){
			addToWhitelist(key);
			context.write(new Text(key.toString()), new IntWritable(sum));
			context.getCounter(Counters.FREQUENT_ITEMSETS).increment(1);
		} else{
			context.getCounter(Counters.DECLINED_SETS).increment(1);
		}
	}

    /**
     * Finishing the frequent itemset extraction for the given tuple size by writing the whitelist ti the filesystem.
     *
     * @param context   			context of mapper
     * 
     * 
     * @throws IOException			is called in a file system context
     * @throws InterruptedException	executed as thread, therefore can be interrupted
	 *
     * @see 						AssociationRules.util.Utils#serializeObject(Object, FileSystem, String)
     *
     */
	@Override
	public void cleanup(Context context) throws IOException, InterruptedException{
		// System.out.println("Whitelist: ");
		// System.out.println(whitelist.toString());

		// write whitelist to fs for use in the next mapper iteration 
		FileSystem fs = null;
		try{
			fs = FileSystem.get(context.getConfiguration());
			Utils.serializeObject(whitelist, fs, basePath+"whitelist_"+(tupelSize)+"_tupel.ser");
			System.out.println("Whitelist persisted for "+(tupelSize)+"-Tupel extraction with size "+whitelist.size()+" and cardinality "+whitelist.cardinality()+".");
		} catch(Exception e){
			System.err.println("Whitelist not persisted for "+(tupelSize)+"-Tupel extraction.");
		}

		// log counters
		System.out.println("CandidateReducer accepted sets: " + context.getCounter(Counters.FREQUENT_ITEMSETS).getValue());
		System.out.println("CandidateReducer declined sets: " + context.getCounter(Counters.DECLINED_SETS).getValue());

	}

    /**
     * Add an itemset to the whitelist. The set is converted into a hash-code which is used as a positional pointer
     * on the BitSet that represents the whitelist.
     *  
     * @param key 	input itemset to add to the whitelist
     * @see 		AssociationRules.util.Utils#hashKey(String)
     *
     */
	private void addToWhitelist(Text key){
		Integer hash = Utils.hashKey(key.toString());
		whitelist.set(hash);
	}
}
