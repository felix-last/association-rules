package AssociationRules.combiners;

//  if counter of mapper needs to be accessed import mapper class:
// import AssociationRules.mappers.CandidateMapper;

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
	*						key		: Itemset <{itemset}>, e.g. A;B;C
	*						value	: 1	(count is always 1 ;) ) 
	*
	*		OUTPUT FORMAT
	*						key		: frequent itemset <{itemset}>, e.g. A;B;C
	*						value	: frequency of itemset <frequency>, e.g. 5 
	*
	*/

	// key is subset of a basket, values are the counts
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		/* 
		* PROBLEM!! The counter that is used is global, hence will always have the number of total inputs...
		* PROBLEM!! need to find a way to count the lines processed per node (--> that get send to one combiner)
		* PROBLEM!! Actually this might also not work
		* PROBLEM!! Combiner doesn't wait until ever mapper ist finished, but until a certain amount of buffer is filled
		* PROBLEM!! Source: https://wiki.apache.org/hadoop/HadoopMapReduce
		*/
		// equivalent to SON algorithm: only output those keys whose count exceeds a partial support threshold
		// long numProcessedBaskets = context.getCounter(CandidateMapper.Counters.INPUTLINES).getValue();
		// System.out.println("[Combiner] Number processed baskets: " + numProcessedBaskets);
		// support threshold needs to be read from job configuration!
		// Double partialSupportThreshold = (double) AssociationRules.SUPPORT_THRESHOLD/numProcessedBaskets;
		// System.out.println("[Combiner] partial support threshold: " + partialSupportThreshold);
		// if (sum >= partialSupportThreshold.intValue()){
			context.write(key, new IntWritable(sum));
		// }
	}
}
