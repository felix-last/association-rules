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


public class CandidateCombiner extends Reducer<Text,IntWritable,Text,IntWritable> {

	/*
	*		INPUT FORMAT
	*						key		: Itemset <{itemset}>, e.g. 1;2;3
	*						value	: 1	(count is always 1 ;) ) 
	*
	*		OUTPUT FORMAT
	*						key		: frequent itemset <{itemset}>, e.g. 1;2;3
	*						value	: frequency of itemset <frequency>, e.g. 5 
	*
	*/


	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}
		context.write(key, new IntWritable(sum));
	}
}
