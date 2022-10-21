# Commands to run the project:
    cd FilteredWordCount
###### create input folder: 
    hadoop fs -mkdir input
###### Upload file to input folder: 
    hadoop fs -copyFromLocal ./file/alice-1.txt input
###### Running MapReduce program:
    hadoop jar './FilteredWordCount.jar' FilteredWordCount input output
###### Copy output from the HDFS:
    hadoop fs -copyToLocal outputwc
    hadoop fs -copyToLocal outputtop200
    hadoop fs -copyToLocal outputaverage
###### To view output:
    hadoop fs -cat outputwc/part-r-00000
    hadoop fs -cat outputtop200/part-r-00000
    hadoop fs -cat outputaverage/part-r-00000
###### Folders cleanup:
    hadoop fs -rm -r input
    hadoop fs -rm -r outputwc
    hadoop fs -rm -r outputtop200
    hadoop fs -rm -r outputaverage
