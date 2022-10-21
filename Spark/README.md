# Commands to run the project:
    cd WordCountScala

###### create input folder: 
    hadoop fs -mkdir input

###### Upload file to input folder: 
    hadoop fs -copyFromLocal './file/alice-1.txt' input

###### Running MapReduce program:
    spark-shell -i './WordCountScala.scala'
    
###### Copy output from the HDFS:
    hadoop fs -copyToLocal top200words
    hadoop fs -copyToLocal average_word_length
    hadoop fs -copyToLocal top10_wordlengths

###### To view output:
    hadoop fs -cat top200words/part-r-00000
    hadoop fs -cat average_word_length/part-r-00000
    hadoop fs -cat top10_wordlengths/part-r-00000

###### Folders cleanup:
    hadoop fs -rm -r inputFile
    hadoop fs -rm -r top200words
    hadoop fs -rm -r average_word_length
    hadoop fs -rm -r top10_wordlengths