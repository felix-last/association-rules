#!/bin/bash

#	-------------------------------------------
#		Version :	1.0.3
#		Date 	:	28.05.2016
#	-------------------------------------------


#	-------------------------------------------
#		Global Variables
#	-------------------------------------------


# set up params
# general
task=""
verbose=false

# task:compile
compileAll=false
mainClass=""
packageName=""

# task:run <Jar Name>.jar <local dataset> <local output path> <additional arguments>
applicationJar=""
inputFile=""
outputPath=""
additionalArguments=""

#task: generate
projectName=""
projectType=""

#	-------------------------------------------
#		Functions
#	-------------------------------------------

#allow printing to console
progress_echo () {
    echo "$@" >&3
}

prepare_logging () {
	if [ $task != "" ]; then
		mkdir -p ./log
		echo "" > ./log/last$task.log
		if [ $verbose == false ]; then
			exec > ./log/last$task.log 2>&1
		fi
	fi
}

compile () {
	# echo "Task: $task"
	# echo "main class: $mainClass"
	# echo "verbose: $verbose"
	# echo "compile all: $compileAll"
	# echo "package: $packageName"

	# check that all params are present
	if [ "$mainClass" == "" ]; then
		echo "Error main class not defined"
		echo "Please specify the main class of the java application with -n."
		exit 1;
	fi

	# check if main class is actually present
	if [ ! -e "./src/$mainClass.java" ]; then 
		echo "Error main class not found"
		echo "Please make sure the specified main class exists."
		exit 1;
	fi

	# check that packageName is present
	if [ \( "$compileAll" == "true" -a "$packageName" == "" \) ]; then
		echo "Error package name not defined"
		echo "When using -a | -all, please specify the package name with -p."
		exit 1;
	fi	


	# start compilation
	prepare_logging

	progress_echo "INFO: compiling java files"
	if  [ $compileAll == true ]; then
		if javac $(find ./src -name "*.java") -cp $(hadoop classpath) -d ./; then
			progress_echo "INFO: compiled sources successfully."
		else 
			progress_echo "ERROR: Failed compilation of java sources. See log for errors."
			exit 1;
		fi
	else
		if javac ./src/$mainClass.java -cp $(hadoop classpath) -d ./; then
			progress_echo "INFO: compiled sources successfully."
		else 
			progress_echo "ERROR: Failed compilation of java sources. See log for errors."
			exit 1;
		fi
	fi

	progress_echo "INFO: creating Manifest"
	if [ $compileAll == true ]; then
		echo "Main-Class: $packageName.$mainClass" > ./Manifest.txt
	else 
		echo "Main-Class: $mainClass" > ./Manifest.txt
	fi

	progress_echo "INFO: packaging into executable"
	if [ $compileAll == true ]; then
		jar cfm ./$mainClass.jar ./Manifest.txt $(find ./ -name "*.class")
	else 
		jar cfm ./$mainClass.jar ./Manifest.txt ./*.class 
	fi

	progress_echo "INFO: cleaning up"
	mkdir -p ./bin
	rm -r ./bin/*
	mkdir -p ./bin/build

	if  [ $compileAll == true ]; then
		mv $packageName ./bin/build/
	else
		mv ./$mainClass.class ./bin/build/$mainClass.class
	fi
	mv ./Manifest.txt ./bin/build/Manifest.txt
	mv ./$mainClass.jar ./bin/$mainClass.jar

} # end of compile()

run () {
	# echo "application jar: $applicationJar"
	# echo "input file: $inputFile"
	# echo "output path: $outputPath"
	# echo "verbose: $verbose"

	HDFS_OUTPUTPATH="results"

	# check application jar
	if [ "$applicationJar" == "" ]; then
		echo "Error jar file not specified."
		echo "Please specify the jar file to execute with -j."
		exit 1;
	fi
	if [ "${applicationJar: -4}" != ".jar" ]; then
		applicationJar="$applicationJar.jar"
	fi
	if [ ! -e $applicationJar ]; then
		echo "Error jar file does not exist."
		echo "Please make sure the specified jar file exists."
		exit 1;
	fi

	# check input file
	if [ "$inputFile" == "" ]; then
		echo "Error input file not specified."
		echo "Please specify the input data file with -i."
		exit 1;
	fi
	if [ ! -e $inputFile ]; then
		echo "Error input file does not exist."
		echo "Please make sure the specified input data file exists."
		exit 1;
	fi

	# set local ouputpath if not specified
	if [ "$outputPath" == "" ]; then
		outputPath="results"
	fi

	prepare_logging

	progress_echo "INFO: cleaning dfs and local location: $outputPath"
	# on hdfs
	if hdfs dfs -test -d  $HDFS_OUTPUTPATH ;then #output dir
		hdfs dfs -rm -r $HDFS_OUTPUTPATH
	fi
	#on local
	rm -rf $outputPath

	progress_echo "INFO: uploading dataset to dfs: $inputFile"
	hdfs dfs -mkdir -p data #create directory if not exists
	if hdfs dfs -test -e  $inputFile ;then # remove input file if exists on hdfs
		hdfs dfs -rm $inputFile
	fi
	hdfs dfs -put $inputFile "data/$(basename $inputFile)"

	progress_echo "INFO: executing $applicationJar"
	hadoop jar $applicationJar "data/$(basename $inputFile)" $HDFS_OUTPUTPATH $additionalArguments

	progress_echo "INFO: downloading results to $outputPath"
	hdfs dfs -get $HDFS_OUTPUTPATH $outputPath

	progress_echo "INFO: Done"
	progress_echo ""


} # end of run()

generate () {
	# echo "project name: $projectName"
	# echo "project type: $projectType"


	#check that project name is given
	if [ "$projectName" == "" ]; then
		echo "Error project name not specified."
		echo "Please specify the project name with -pn."
	fi
	
	#check that project type is given
	if [ "$projectType" == "" ]; then
		echo "Error project type not specified."
		echo "Please specify the project name with -t."
	fi
	

	progress_echo "INFO: Creating Directories"
	mkdir -p $projectName
	mkdir -p $projectName/bin
	mkdir -p $projectName/bin/build
	mkdir -p $projectName/log
	mkdir -p $projectName/data
	mkdir -p $projectName/results
	mkdir -p $projectName/src

	progress_echo "INFO: Creating Type specific content"
	case "$projectType" in
		"mapreduce" | "mapred" )
			generate_mapred
		;;
	esac
		


} # end of generate ()

generate_mapred () {

	#additional folder structure
	mkdir -p $projectName/src/mappers
	mkdir -p $projectName/src/combiners
	mkdir -p $projectName/src/reducers

	#package name
	echo "What is the name of the java package?"
	while true; do
    	read -p "	" pn
	    if [ "$pn" != "" ]; then 
	    	break
	    else 
	    	echo "Name cannot be empty."
	    fi
	done

	#main class
	echo "What is the name of the Main Class?"
	while true; do
    	read -p "	" mc
	    if [ "$mc" != "" ]; then 
	    	break
	    else 
	    	echo "Name cannot be empty."
	    fi
	done


	#mapper class
	echo "What is the name of the Mapper Class?"
	while true; do
    	read -p "	" mapc
	    if [ "$mapc" != "" ]; then 
	    	break
	    else 
	    	echo "Name cannot be empty."
	    fi
	done


	#combiner class
	echo "What is the name of the Combiner Class?"
	while true; do
    	read -p "	" comc
	    if [ "$comc" != "" ]; then 
	    	break
	    else 
	    	echo "Name cannot be empty."
	    fi
	done


	#reducer class
	echo "What is the name of the Reducer Class?"
	while true; do
    	read -p "	" redc
	    if [ "$redc" != "" ]; then 
	    	break
	    else 
	    	echo "Name cannot be empty."
	    fi
	done


	# create mainclass
	echo "INFO: Generating file: $pn.$mc"
	mainclassContent="package $pn;\n\nimport $pn.mappers.$mapc;\nimport $pn.combiners.$comc;\nimport $pn.reducers.$redc;"
	mainclassContent="$mainclassContent\n"
	mainclassContent="$mainclassContent\nimport org.apache.hadoop.conf.Configuration;"
	mainclassContent="$mainclassContent\nimport org.apache.hadoop.fs.Path;"
	mainclassContent="$mainclassContent\nimport org.apache.hadoop.io.IntWritable;"
	mainclassContent="$mainclassContent\nimport org.apache.hadoop.io.Text;"
	mainclassContent="$mainclassContent\nimport org.apache.hadoop.mapreduce.Job;"
	mainclassContent="$mainclassContent\nimport org.apache.hadoop.mapreduce.lib.input.FileInputFormat;"
	mainclassContent="$mainclassContent\nimport org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;"
	mainclassContent="$mainclassContent\nimport org.apache.hadoop.util.GenericOptionsParser;"
	mainclassContent="$mainclassContent\n\n"
	mainclassContent="$mainclassContent\npublic class $mc {"
	mainclassContent="$mainclassContent\n	public static void main(String[] args) throws Exception {"
	mainclassContent="$mainclassContent\n   	Configuration conf = new Configuration();"
	mainclassContent="$mainclassContent\n   	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();"
	mainclassContent="$mainclassContent\n   	if (otherArgs.length != 2) {"
	mainclassContent="$mainclassContent\n   		System.err.println(\"Usage: wordcount <in> <out>\");"
	mainclassContent="$mainclassContent\n   		System.exit(2);"
	mainclassContent="$mainclassContent\n   	}"
	mainclassContent="$mainclassContent\n   	Job job = new Job(conf, \"$projectName\");"
	mainclassContent="$mainclassContent\n   	job.setJarByClass($mc.class);"
	mainclassContent="$mainclassContent\n   	job.setMapperClass($mapc.class);"
	mainclassContent="$mainclassContent\n   	job.setCombinerClass($comc.class);"
	mainclassContent="$mainclassContent\n   	job.setReducerClass($redc.class);"
	mainclassContent="$mainclassContent\n   	job.setOutputKeyClass(Text.class);"
	mainclassContent="$mainclassContent\n   	job.setOutputValueClass(IntWritable.class);"
	mainclassContent="$mainclassContent\n   	FileInputFormat.addInputPath(job, new Path(otherArgs[0]));"
	mainclassContent="$mainclassContent\n   	FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));"
	mainclassContent="$mainclassContent\n   	System.exit(job.waitForCompletion(true) ? 0 : 1);"
	mainclassContent="$mainclassContent\n   }"
	mainclassContent="$mainclassContent\n}"
	echo -e $mainclassContent > "$projectName/src/$mc.java"

	# create mapper class
	echo "INFO: Generating file: $pn.mappers.$mapc"
	mapperclassContent="package $pn.mappers;"
	mapperclassContent="$mapperclassContent\n"
	mapperclassContent="$mapperclassContent\nimport java.io.IOException;"
	mapperclassContent="$mapperclassContent\nimport java.util.StringTokenizer;"
	mapperclassContent="$mapperclassContent\nimport org.apache.hadoop.io.IntWritable;"  
	mapperclassContent="$mapperclassContent\nimport org.apache.hadoop.io.Text;"  
	mapperclassContent="$mapperclassContent\nimport org.apache.hadoop.mapreduce.Mapper;"
	mapperclassContent="$mapperclassContent\n\n"
	mapperclassContent="$mapperclassContent\npublic class $mapc extends Mapper<Object, Text, Text, IntWritable> {"
	mapperclassContent="$mapperclassContent\n	private final static IntWritable one = new IntWritable(1);"
	mapperclassContent="$mapperclassContent\n	private Text word = new Text();"
	mapperclassContent="$mapperclassContent\n	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {"
	mapperclassContent="$mapperclassContent\n			StringTokenizer itr = new StringTokenizer(value.toString());"
	mapperclassContent="$mapperclassContent\n			while (itr.hasMoreTokens()) {"
	mapperclassContent="$mapperclassContent\n				word.set(itr.nextToken());"
	mapperclassContent="$mapperclassContent\n				context.write(word, one);"
	mapperclassContent="$mapperclassContent\n			}"             
	mapperclassContent="$mapperclassContent\n	}"
	mapperclassContent="$mapperclassContent\n}"
	echo -e $mapperclassContent > "$projectName/src/mappers/$mapc.java"


	# create combiner class
	echo "INFO: Generating file: $pn.combiners.$comc"
	combinerclassContent="package $pn.combiners;"
	combinerclassContent="$combinerclassContent\n"
	combinerclassContent="$combinerclassContent\nimport java.io.IOException;"
	combinerclassContent="$combinerclassContent\nimport org.apache.hadoop.io.IntWritable;"
	combinerclassContent="$combinerclassContent\nimport org.apache.hadoop.io.Text;"
	combinerclassContent="$combinerclassContent\nimport org.apache.hadoop.mapreduce.Reducer;"
	combinerclassContent="$combinerclassContent\n\n"
	combinerclassContent="$combinerclassContent\npublic class $comc extends Reducer<Text,IntWritable,Text,IntWritable> {"
	combinerclassContent="$combinerclassContent\n	private IntWritable result = new IntWritable();"
	combinerclassContent="$combinerclassContent\n	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {"
	combinerclassContent="$combinerclassContent\n		int sum = 0;"
	combinerclassContent="$combinerclassContent\n		for (IntWritable val : values) {"
	combinerclassContent="$combinerclassContent\n			sum += val.get();"
	combinerclassContent="$combinerclassContent\n		}"
	combinerclassContent="$combinerclassContent\n		result.set(sum);"
	combinerclassContent="$combinerclassContent\n		context.write(key, result);"
	combinerclassContent="$combinerclassContent\n	}"
	combinerclassContent="$combinerclassContent\n}"
	echo -e $combinerclassContent > "$projectName/src/combiners/$comc.java"


	#create reducer class
	echo "INFO: Generating file: $pn.reducers.$redc"
	reducerclassContent="package $pn.reducers;"
	reducerclassContent="$reducerclassContent\n"
	reducerclassContent="$reducerclassContent\nimport java.io.IOException;"
	reducerclassContent="$reducerclassContent\nimport org.apache.hadoop.io.IntWritable;"
	reducerclassContent="$reducerclassContent\nimport org.apache.hadoop.io.Text;"
	reducerclassContent="$reducerclassContent\nimport org.apache.hadoop.mapreduce.Reducer;"
	reducerclassContent="$reducerclassContent\n\n"
	reducerclassContent="$reducerclassContent\npublic class $redc extends Reducer<Text,IntWritable,Text,IntWritable> {"
	reducerclassContent="$reducerclassContent\n	private IntWritable result = new IntWritable();"	
	reducerclassContent="$reducerclassContent\n	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {"
	reducerclassContent="$reducerclassContent\n		int sum = 0;"
	reducerclassContent="$reducerclassContent\n		for (IntWritable val : values) {"
	reducerclassContent="$reducerclassContent\n			sum += val.get();"
	reducerclassContent="$reducerclassContent\n		}"
	reducerclassContent="$reducerclassContent\n		result.set(sum);"
	reducerclassContent="$reducerclassContent\n		context.write(key, result);"
	reducerclassContent="$reducerclassContent\n	}"
	reducerclassContent="$reducerclassContent\n}"
	echo -e $reducerclassContent > "$projectName/src/reducers/$redc.java"


	echo "INFO: project is ready."

} # end of generate_mapred ()

usage () {
	echo "Hadoop Utility - Usage"
	echo "Use $ sh compile.sh <task> [OPTIONS]"
	echo ""
	echo "Tasks"
	echo "	compile		: use to compile and package a java project into an executable jar"
	echo "		OPTIONS (required):"
	echo "			-n | --name 	: specify the name of the main class"
	echo "		OPTIONS (optional):"
	echo "			-v | --verbose 	: log everything to console"
	echo "			-a | --all 	: compile all .java files in the ./src folder, requires package name"
	echo "			-p | --package 	: specify containing package name"
	echo ""
	echo "	run		: execute jar application on hadoop cluster"
	echo "		OPTIONS (required):"
	echo "			-j | --jar 	: specify the jar to execute"
	echo "			-i | --input	: specify the data file to process"
	echo "		OPTIONS (optional):"
	echo "			-o | --output	: specify the output directory, if empty will be set to ./results"
	echo "			     --args 	: add additional programm related arguments"
	echo "			            	  NOTE: all arguments after this will be passed to the jar application"
	echo "			-v | --verbose 	: log everything to console"
	echo ""
	echo "	generate	: generate a new hadoop application skeleton"
	echo "		OPTIONS (required):"
	echo "			-pn | --projectname : Name of the hadoop application project"
	echo "			-t | --type : [mapreduce,...]"
	# echo "		OPTIONS (optional):"
	# echo "			-v | --verbose 	: log everything to console"
}
#	-------------------------------------------
#		Main
#	-------------------------------------------

exec 3>&1 # enable progress_echo

# retrieve task
task=$1
shift

javaArgsFound="false"
# retrieve configuration
while [ \( "$1" != "" \) -a \( "$javaArgsFound" == "false" \) ]; do
    case $1 in
        -a | --all )
			compileAll=true;;
        -n | --name )
			shift
			mainClass=$1;;
        -p | --package )
			shift
			packageName=$1;;
        -j | --jar )
			shift
			applicationJar=$1;;
        -i | --input )
			shift
			inputFile=$1;;
        -o | --output )
			shift
			outputPath=$1;;
        --args )
			shift
			javaArgsFound="true"
			additionalArguments="$@";;
        -pn | --projectname )
			shift
			projectName=$1;;
        -t | --type )
			shift
			projectType=$1;;
        -v | --verbose )
			verbose=true;;
        -h | --help )
			echo "help"
			usage
            exit;;
        * )
			usage
            exit 1
    esac
    shift
done


case "$task" in
	"compile" )
		compile
		exit
	;;
	"run" )
		run
		exit
	;;
	"generate" )
		generate
		exit
	;;
	* )
		usage
		exit
esac
