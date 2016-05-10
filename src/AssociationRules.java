package AssociationRules;

// package dependencies
import AssociationRules.util.Utils;
import AssociationRules.mappers.CandidateMapper;
import AssociationRules.combiners.CandidateCombiner;
import AssociationRules.reducers.CandidateReducer;
import AssociationRules.mappers.AssociationMapper;
import AssociationRules.reducers.AssociationReducer;

// hadoop dependencies
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// general dependencies



public class AssociationRules {


	public static final int MIN_NUMBER_ELEMENTS = 2; // less than 2 doesn't make sense, need at least 2 elements to construct a rule ;)
	
	// commandline paramters:
	private static int SUPPORT_THRESHOLD = 0;
	private static int MAX_DEGREE = 1; // how many independent elements per rule should be used? e.g. 1: A==>B,C... or 3: A,B,C==>X,Y
	

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length > 3 || otherArgs.length < 2) {
			printUsage();
			System.exit(2);
		}

		final String initialInputPath 	= otherArgs[0];
		final String outputPath 		= otherArgs[1];

		try{
			SUPPORT_THRESHOLD = Integer.parseInt(otherArgs[2].split("\\+")[0]);
			MAX_DEGREE = Integer.parseInt(otherArgs[2].split("\\+")[1]);
		} catch(Exception e){
			// ignore failure use default paramters
			System.out.println("INFO: couldn't convert additional parameters, using defaults");
		}


		System.out.println("      ***********************************");
		System.out.println("INFO: running extraction with");
		System.out.println("		support threshold 	= " + SUPPORT_THRESHOLD);
		System.out.println("		maximum degree   	= " + MAX_DEGREE);
		System.out.println("      ***********************************");

		// calculate frequent itemsets
		extractFrequentItems(initialInputPath, outputPath+"/frequentItemSets/");

		// calculate association rules from frequent itemsets
		extractAssociationRules(outputPath+"/frequentItemSets/", outputPath+"/rules");

	}


	/*		METHOD: extractFrequentItems
	*					String inputPath :	input data files containing "."-separated basket data
	*					String outputPath:	output location of the extracted frequent itemsets
	*		
	*		Extracts itemsets from baskets and calculates frequency (result dependent on support threshold)
	*/
	public static void extractFrequentItems(String inputPath, String outputPath) throws Exception{
		System.out.println("************************************************");
		System.out.println("INFO: starting frequent itemset extraction.");
		Configuration conf = new Configuration();
		conf.set("SUPPORT_THRESHOLD", ""+SUPPORT_THRESHOLD);
		conf.set("MAX_DEGREE", ""+MAX_DEGREE);
		conf.set("MIN_NUMBER_ELEMENTS", ""+MIN_NUMBER_ELEMENTS);
		Job job = new Job(conf, "AssociationRules_ExtractFrequentItems");
		job.setJarByClass(AssociationRules.class);
		job.setMapperClass(CandidateMapper.class);
		job.setCombinerClass(CandidateCombiner.class);
		job.setReducerClass(CandidateReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		job.waitForCompletion(true);
		System.out.println("INFO: frequent itemset extraction completed.");
		System.out.println("INFO: number of processed baskets : " + job.getCounters().findCounter(CandidateMapper.Counters.INPUTLINES).getValue());
		System.out.println("INFO: number of processed itemsets: " + job.getCounters().findCounter(CandidateMapper.Counters.POWERSETS).getValue());
		System.out.println("INFO: number of frequent itemsets : " + job.getCounters().findCounter(CandidateReducer.Counters.FREQUENT_ITEMSETS).getValue());
		System.out.println("************************************************");
	}
	

	/*		METHOD: extractAssociationRules
	*					String inputPath :	input files containing frequent itemsets, e.g. "A;C;D	3"
	*					String outputPath:	output location of the extracted rules
	*		
	*		Extracts association rules based on frequent itemsets with a maximum number of independent items (max degree)
	*/
	public static void extractAssociationRules(String inputPath, String outputPath) throws Exception{
		System.out.println("************************************************");
		System.out.println("INFO: starting rules extraction.");
		Configuration conf = new Configuration();
		conf.set("SUPPORT_THRESHOLD", ""+SUPPORT_THRESHOLD);
		conf.set("MAX_DEGREE", ""+MAX_DEGREE);
		conf.set("MIN_NUMBER_ELEMENTS", ""+MIN_NUMBER_ELEMENTS);
		Job job = new Job(conf, "AssociationRules_ExtractAssociationRules");
		job.setJarByClass(AssociationRules.class);
		job.setMapperClass(AssociationMapper.class);
		job.setReducerClass(AssociationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		job.waitForCompletion(true);
		System.out.println("INFO: rule extraction completed.");
		System.out.println("INFO: total number of itemset permutations: " + job.getCounters().findCounter(AssociationMapper.Counters.PERMUTATIONS).getValue());
		System.out.println("INFO: total number of extracted rules     : " + job.getCounters().findCounter(AssociationReducer.Counters.RULES).getValue());
		System.out.println("************************************************");
	}

	private static void printUsage(){
		System.err.println("Usage: AssociationRules <in> <out> [<support threshold>+<max degree>]");
		System.err.println("       e.g. AssociationRules data/sample.txt results 15+3]");
	}
}
