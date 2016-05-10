package AssociationRules.reducers;

// package dependencies
import AssociationRules.util.Utils;

// hadoop dependencies
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// general dependencies
import java.util.Map;
import java.util.HashMap;
import java.io.IOException;
import java.lang.InterruptedException;


public class CandidateReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

	/*
	*		INPUT FORMAT
	*						key		: Itemset <{itemset}>, e.g. A;B;C
	*						value	: 1	(count is always 1 ;) ) 
	*
	*		OUTPUT FORMAT
	*						key		: frequent itemset <{itemset}>, e.g. A;B;C
	*						value	: frequency of itemset <frequency>, e.g. 5 
	*
	*/

	public static enum Counters{
		FREQUENT_ITEMSETS
	}

	// by using this map the output of the reducer can be sorted by frequency
	private Map<Text, IntWritable> resultMap = new HashMap<>();

	// key is subset of a basket, values are the counts
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		// context.write(key, new IntWritable(sum));
		// don't write on context but insert into result map
		resultMap.put(new Text(key), new IntWritable(sum));
	}

	public void cleanup(Context context) throws IOException, InterruptedException{
		int supportThreshold = Integer.parseInt(context.getConfiguration().get("SUPPORT_THRESHOLD"));
		System.out.println("CandidateReducer: support threshold = " + supportThreshold);
		// sort keys by frequency and writes to context
        Map<Text, IntWritable> sortedResultMap = Utils.sortMapByValues(resultMap);

        int numTotal = 0;

        for (Text key : sortedResultMap.keySet()) {
        	numTotal++;
        	if (sortedResultMap.get(key).get() >= supportThreshold){
        		context.write(key, sortedResultMap.get(key));
        		context.getCounter(Counters.FREQUENT_ITEMSETS).increment(1);
    		}
        }

        System.out.println("Cleanup: Number of distinct Subsets = " + numTotal);
        System.out.println("Cleanup: Number of discarded Subsets = " + (numTotal-context.getCounter(Counters.FREQUENT_ITEMSETS).getValue()));

	}
}
