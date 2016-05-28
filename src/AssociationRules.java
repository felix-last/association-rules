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

/**
 * AssociationRules is the Main Class that executes the Map-Reduce
 * Application.
 * The class handles the management of the input files of the 
 * Map-Reduce Application, the parsing of the parameters (such as
 * support threshold, confidence threshold or whether to keep 
 * helperfiles) and the execution of the different types of jobs 
 * necessary for generating association rules from a basket dataset.
 *
 * @author      Lukas Fahr
 * @author      Felix Last
 * @author      Paul Englert
 * @version     1.0 - 28.05.2016
 * @since       1.0
 */
public class AssociationRules {

	/**
     * The regular expression that separates items in the input baskets.
     */
	private static final String BASKET_ITEM_SPLITTER = ",";

	/**
     * The character sequence that separates the antedecent from the consequent in the rules.
     */
	private static final String RULE_COMPONENT_DELIMITER = "->";

	/**
     * The character sequence that separates the items of the antidecent (and consequent).
     */
	private static final String RULE_ITEM_SEPARATOR = ",";


	/**
     * The start time of the application.
     */
	private static long APPLICATION_START_TIME = System.currentTimeMillis() / 1000L;

	/**
     * The flag whether to keep or delete the generated intermediary files (commandline accessible).
     */
	private static Boolean KEEP_HELPER_FILES = true;

	
	/**
     * The support threshold for the frequent itemset extraction (commandline accessible).
     */
	private static int SUPPORT_THRESHOLD = 100;

	/**
     * The confidence threshold for the rules extraction (commandline accessible).
     */
	private static double CONFIDENCE_THRESHOLD = 0.3;


	
	/**
     * The main method handling the global process.
     * Handles the parsing of the commandline parameters and continues with executing
     * the frequent itemset extraction and then the rules extraction. The overall 
     * runtime of the application will be recorded and printed to the console.
     * If the number of parameters is not as expected the usage of the application will
     * be printed to the console and the application terminates.
     *
     * @param args	the commandline parameters: support threshold, confidence threshold 
     *				and the keep helperfiles flag
     * @throws Exception	possible exceptions: IOException, InterruptedException
     */
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length > 5 || otherArgs.length < 2) {
			printUsage();
			System.exit(2);
		}

		final String initialInputPath 	= otherArgs[0];
		final String outputPath 		= otherArgs[1];

		try{
			SUPPORT_THRESHOLD = Integer.parseInt(otherArgs[2]);
			CONFIDENCE_THRESHOLD = Double.parseDouble(otherArgs[3]);
			KEEP_HELPER_FILES = Boolean.parseBoolean(otherArgs[4]);
		} catch(Exception e){
			// ignore failure use default paramters
			System.out.println("INFO: problem converting additional parameters, using defaults one or more default values");
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

		long endTime = System.currentTimeMillis() / 1000L;
		String runtime = ""+ (int)Math.floor((endTime-APPLICATION_START_TIME)/60)+":"+(endTime-APPLICATION_START_TIME)%60+"mins";
		System.out.println("Application runtime: "+runtime);
	}


		
	/**
     * Executing a frequent itemset extraction with fixed tuple size for the specified input.
     * The method uses the private members SUPPORT_THRESHOLD and BASKET_ITEM_SPLITTER and the
     * parameters to extract all frequent itemsets with the specified tuple size.
     * In order to achieve that a job object is created with all the necessary configurations.
     * 
     *
     * @param inputPath		the file system location of the input files 
     * @param outputPath	the desired output location on the file system 
     * @param tupelSize		the size of the itemsets that should be extracted 
     * @param tmpPath		the file system location for the intermediary files 
     * @return				number of frequent itemsets that have been extracted
     * @throws Exception	possible exceptions: IOException, InterruptedException
	 *
     * @see       			#main(String[])
     *					
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
	

		
	/**
     * Executing a association rules extraction from a dataset of frequent items.
     * The method uses the private members CONFIDENCE_THRESHOLD, RULE_COMPONENT_DELIMITER
     * and RULE_ITEM_SEPARATOR and the specified parameters to compute association rules
     * from all sub-directories (with the use of maxTupelSize) of the inputPath.
     * In order to achieve that a job object is created with all the necessary configurations
     * and the sub-directories are added as input paths.
     * 
     *
     * @param inputPath		the file system location of the input files 
     * @param outputPath	the desired output location on the file system 
     * @param maxTupelSize	the size of the itemsets that should be extracted 
     * @param tmpPath		the file system location for the intermediary files
     * @throws Exception	possible exceptions: IOException, InterruptedException
	 *
     * @see       			#main(String[])
     * @see       			#extractFrequentItems(String, String, int, String)
     *					
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
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
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
		
	/**
     * Cleaning the file system from the intermediary files used during the execution.
     * If the private member KEEP_HELPER_FILES is set to <code>true</code> all files, except
     * for the readable version of the key item mapping will be deleted. This method
     * is called after the map-reduce jobs have been executed and the application
     * is about to terminate.
     *
     * @param outputPath	the desired output location on the file system 
	 *
     * @see       			#main(String[])
     *					
     */
	private static void cleanup(String outputPath){
		if (!KEEP_HELPER_FILES){
			try{
				FileSystem fs = FileSystem.get(new Configuration());
				Path path;
				// delete mappings
				path = new Path(outputPath+"/helperfiles/item-keyMap.ser");
		        if (fs.exists(path)) {
		            fs.delete(path, false);
		        }
		        path = new Path(outputPath+"/helperfiles/key-itemMap.ser");
		        if (fs.exists(path)) {
		            fs.delete(path, false);
		        }
		        // delete whitelists
		        boolean stop = false;
		        int i = 0;
		        while (!stop){
		        	i++;	
		        	path = new Path(outputPath+"/helperfiles/whitelist_"+i+"_tupel.ser");
			        if (fs.exists(path)) {
			            fs.delete(path, false);
			        } else {
			        	stop = true;
			        }
		        }
		        // delete output of first job of last iteration (is empty file ;) )
		        path = new Path(outputPath+"/frequentItemSets/"+(i-1)+"-tupel/");
		        if (fs.exists(path)){
		        	fs.delete(path, true);
		        }
		    } catch(Exception e){
		    	//
		    }
		}
	}

	/**
     * Print the usage of the application to the console.
	 *
     * @see       			#main(String[])
     *					
     */
	private static void printUsage(){
		System.err.println("Usage: AssociationRules <in> <out> [<support threshold> <confidence threshold>] [<keep helperfiles: true|false>]");
		System.err.println("       e.g. AssociationRules data/sample.txt results");
		System.err.println("       e.g. AssociationRules data/sample.txt results 15 0.4 true");
	}
}
