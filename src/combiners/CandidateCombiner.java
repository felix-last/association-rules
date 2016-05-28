package AssociationRules.combiners;

// package dependencies
import AssociationRules.util.Utils;

// hadoop dependencies
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

// general dependencies
import java.io.IOException;
import java.lang.InterruptedException;


/**
 * CandidateCombiner is the combiner used for frequent item extraction.
 * The class handles the summation of the counts of itemsets.
 *
 * @author      Lukas Fahr
 * @author      Felix Last
 * @author      Paul Englert
 * @version     1.0 - 28.05.2016
 * @since       1.0
 */
public class CandidateCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {
	
	/**
     * Sum up the counts for a given key itemset.<p>
     * INPUT FORMAT<p>
	 * key	: Itemset, e.g. 1;2;3<br>
	 * value: 1	(count is always 1 ;) ) <p>
	 *
	 * OUTPUT FORMAT<p>
	 * key	: frequent itemset, e.g. 1;2;3<br>
	 * value: frequency of itemset, e.g. 5 <p>
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
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
