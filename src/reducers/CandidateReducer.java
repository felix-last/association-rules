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

	// whitelist for SON approach
	private static BitSet whitelist = new BitSet();

	// support threshold
	private static int supportThreshold = 0;

	// helper files location
	private static String basePath = "";

	// tupel size of current iteration
	private static Integer tupelSize = 0;


	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// read configuration
		supportThreshold = Integer.parseInt(context.getConfiguration().get("SUPPORT_THRESHOLD"));
		basePath = context.getConfiguration().get("TMP_FILE_PATH");
		tupelSize = Integer.parseInt(context.getConfiguration().get("TUPEL_SIZE"));
	}

	// key is subset of a basket, values are the counts
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

	private void addToWhitelist(Text key){
		Integer hash = Utils.hashKey(key.toString());
		whitelist.set(hash);
	}
}
