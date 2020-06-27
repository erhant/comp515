'''
Part 4. 
Write a program to replace spaces with dash character in book.txt and output to words-.txt

Run using: $spark-submit 
'''

# run this file with spark-submit.cmd
import re # regex for words
from pyspark import SparkContext, SparkConf

if __name__ == "__main__":

    ### Create Spark Context
    conf = SparkConf().setAppName("Part1").setMaster("local[*]") # use all available cores
    sc = SparkContext(conf = conf)
    
    ### Load RDD
    lines = sc.textFile("./in/book.txt")
    
    ### Transform on RDD
    # Split the content with space or tab characters
    # Re-join them using dash character
    words = lines.map(lambda line: "-".join(re.split(r" |\t", line)))

    ### Output RDD
    words.saveAsTextFile("./out/words-.txt")
    print("Saved the results to ./out/words-.txt")