val inputFile = sc.textFile("file:///home/mdm77/Code/Spark/alice-1.txt")

val stopWords = List[String]("", "i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now")

val stringTokens = inputFile.flatMap(d => d.split(" "))

val cleanedStringTokens = stringTokens.map(d => d.replaceAll("[^a-zA-Z0-9\\s]", ""))

val filteredTokens = cleanedStringTokens.map(d => d.trim().toLowerCase()).filter(d => !stopWords.contains(d))

val stringCount = filteredTokens.map(d => (d, 1))

val tokensCount = stringCount.reduceByKey(_+_)

val sortedWords = tokensCount.sortBy(r => (-r._2, r._1)).take(200)

sc.parallelize(sortedWords).saveAsTextFile("top200words")

// System.out.println(sortedWords.mkString("\n"))

// System.out.println("-------------------------------------------")

val wordCharCount = filteredTokens.map(d => (d.charAt(0), d.length))

val groupedCharCounts = wordCharCount.groupBy(_._1)

val aggregatedWordCounts = groupedCharCounts.map{ case(char, charList) => (char, charList.size, charList.map(_._2).sum) }

val averageWordLength = aggregatedWordCounts.map(x => (x._1, x._3.asInstanceOf[Double]/x._2)).sortBy(_._1)

averageWordLength.saveAsTextFile("average_word_length")

// System.out.println(averageWordLength.collect().mkString("\n"))

// System.out.println("-------------------------------------------")

val top10AverageWordLengths = aggregatedWordCounts.map(x => (x._1, x._2, x._3.asInstanceOf[Double]/x._2)).sortBy(-_._3).take(10)

sc.parallelize(top10AverageWordLengths).saveAsTextFile("top10_wordlengths")

// System.out.println(top10AverageWordLengths.mkString("\n"))