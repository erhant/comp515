'''
Part 3. 
Using Spark, wrtie a program to lowercase all words in book.txt and output to words_lower.txt

Run using: $spark-submit 
'''

# run this file with spark-submit.cmd
import re # regex for words
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    ### Create Spark Context
    conf = SparkConf().setAppName("Part3").setMaster("local[*]") # use all available cores
    sc = SparkContext(conf = conf)
    
    ### Load RDD
    lines = sc.textFile("./in/book.txt") # get the article
    
    ### Transform on RDD
    # split words using the delimiter as whitespace.
    # then filter those that have non word characters.
    words = lines.map(lambda line: line.lower())

    ### Output RDD
    words.saveAsTextFile("./out/words_lower.txt")
    print("Saved the results to ./out/words_lower.txt")
