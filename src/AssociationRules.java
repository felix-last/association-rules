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
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;

// general dependencies



public class AssociationRules {

	private static final String BASKET_ITEM_SPLITTER = ","; // regex to split baskets into items, depending on input file
	private static final String RULE_COMPONENT_DELIMITER = "->";
	private static final String RULE_ITEM_SEPARATOR = ",";


	private static final Boolean KEEP_HELPER_FILES = false;

	// commandline paramters:
	private static int SUPPORT_THRESHOLD = 100;
	private static double CONFIDENCE_THRESHOLD = 0.3;
	
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
			CONFIDENCE_THRESHOLD = Double.parseDouble(otherArgs[2].split("\\+")[1]);
		} catch(Exception e){
			// ignore failure use default paramters
			System.out.println("INFO: couldn't convert additional parameters, using defaults");
		}


		System.out.println("      ***********************************");
		System.out.println("INFO: running extraction with");
		System.out.println("		support threshold   	= " + SUPPORT_THRESHOLD);
		System.out.println("		confidence threshold 	= " + CONFIDENCE_THRESHOLD);
		System.out.println("      ***********************************");

		// calculate frequent itemsets
		boolean run = true;
		int i = 1;
		while (run){
			Long freqItemCount = extractFrequentItems(initialInputPath, outputPath+"/frequentItemSets/"+i+"-tupel/", i, outputPath+"/helperfiles/");
			i++;
			if (freqItemCount == 0) {
				run = false;
			}
		}
		System.out.println("Stopped after trying with "+(i-1)+"-tupels");

		// calculate association rules from frequent itemsets
		extractAssociationRules(outputPath+"/frequentItemSets/", outputPath+"/rules", (i-1), outputPath+"/helperfiles/");


		// cleanup
		cleanup(outputPath);
	}


	/*		METHOD: extractFrequentItems
	*					String inputPath :	input data files containing "."-separated basket data
	*					String outputPath:	output location of the extracted frequent itemsets
	*		
	*		Extracts itemsets from baskets and calculates frequency (result dependent on support threshold)
	*/
	public static Long extractFrequentItems(String inputPath, String outputPath, int tupelSize, String tmpPath) throws Exception{
		System.out.println("************************************************");
		System.out.println("INFO: starting frequent itemset extraction for "+tupelSize+"-tupels.");
		Configuration conf = new Configuration();
		conf.set("SUPPORT_THRESHOLD", ""+SUPPORT_THRESHOLD);
		conf.set("TUPEL_SIZE", ""+tupelSize);
		conf.set("BASKET_ITEM_SPLITTER", BASKET_ITEM_SPLITTER);
		conf.set("TMP_FILE_PATH", tmpPath);
		Job job = new Job(conf, "AssociationRules_ExtractFrequentItems_"+tupelSize+"-tupel");
		job.setJarByClass(AssociationRules.class);
		job.setMapperClass(CandidateMapper.class);
		job.setCombinerClass(CandidateCombiner.class);
		job.setReducerClass(CandidateReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		job.waitForCompletion(true);
		System.out.println("INFO: frequent itemset extraction completed for tupel size: "+tupelSize);
		System.out.println("INFO: number of processed baskets : " + job.getCounters().findCounter(CandidateMapper.Counters.INPUTLINES).getValue());
		System.out.println("INFO: number of passed itemsets: " + job.getCounters().findCounter(CandidateMapper.Counters.WRITTENSETS).getValue());
		System.out.println("INFO: number of frequent itemsets : " + job.getCounters().findCounter(CandidateReducer.Counters.FREQUENT_ITEMSETS).getValue());
		System.out.println("************************************************");
		return job.getCounters().findCounter(CandidateReducer.Counters.FREQUENT_ITEMSETS).getValue();
	}
	

	/*		METHOD: extractAssociationRules
	*					String inputPath :	input files containing frequent itemsets, e.g. "A;C;D	3"
	*					String outputPath:	output location of the extracted rules
	*		
	*		Extracts association rules based on frequent itemsets with a maximum number of independent items (max degree)
	*/
	public static void extractAssociationRules(String inputPath, String outputPath, int maxTupelSize, String tmpPath) throws Exception{
		System.out.println("************************************************");
		System.out.println("INFO: starting rules extraction.");
		Configuration conf = new Configuration();
		conf.set("CONFIDENCE_THRESHOLD", ""+CONFIDENCE_THRESHOLD);
		conf.set("MAX_TUPEL_SIZE", ""+maxTupelSize);
		conf.set("RULE_COMPONENT_DELIMITER", RULE_COMPONENT_DELIMITER);
		conf.set("RULE_ITEM_SEPARATOR", RULE_ITEM_SEPARATOR);
		conf.set("TMP_FILE_PATH", tmpPath);
		Job job = new Job(conf, "AssociationRules_ExtractAssociationRules");
		job.setJarByClass(AssociationRules.class);
		job.setMapperClass(AssociationMapper.class);
		job.setReducerClass(AssociationReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		// ignore the path for maxTupelSize, since that one is empty per definition ;)
		for (int i = 1; i < maxTupelSize; i++){ 
			FileInputFormat.addInputPath(job, new Path(inputPath+""+i+"-tupel/"));
		}

		FileOutputFormat.setOutputPath(job, new Path(outputPath));		
		job.waitForCompletion(true);
		System.out.println("INFO: rule extraction completed.");
		System.out.println("INFO: total number of itemset permutations: " + job.getCounters().findCounter(AssociationMapper.Counters.PERMUTATIONS).getValue());
		System.out.println("INFO: total number of extracted rules     : " + job.getCounters().findCounter(AssociationReducer.Counters.RULES).getValue());
		System.out.println("************************************************");
	}

	private static void cleanup(String outputPath){
		if (!KEEP_HELPER_FILES){
			try{
				FileSystem fs = FileSystem.get(new Configuration());
				Path path;
				// delete mappings
				path = new Path(outputPath+"/helperfiles/item-keyMap.ser");
		        if (fs.exists(path)) {
		            fs.delete(path, true);
		        }
		        path = new Path(outputPath+"/helperfiles/key-itemMap.ser");
		        if (fs.exists(path)) {
		            fs.delete(path, true);
		        }
		        // delete whitelists
		        boolean stop = false;
		        int i = 0;
		        while (!stop){
		        	i++;	
		        	path = new Path(outputPath+"/helperfiles/whitelist_"+i+"_tupel.ser");
			        if (fs.exists(path)) {
			            fs.delete(path, true);
			        } else {
			        	stop = true;
			        }
		        }
		    } catch(Exception e){
		    	//
		    }
		}
	}

	private static void printUsage(){
		System.err.println("Usage: AssociationRules <in> <out> [<support threshold>+<confidence threshold>]");
		System.err.println("       e.g. AssociationRules data/sample.txt results");
		System.err.println("       e.g. AssociationRules data/sample.txt results 15+0.4");
	}
}
