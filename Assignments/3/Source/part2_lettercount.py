'''
Part 2. 
Using Spark, write a program to count the occurences of each english letter in book.txt. 

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
    lines = sc.textFile("./in/book.txt") # get the article
    
    ### Transform on RDD
    # Convert lines to lowercase (because A and a are the same letter)
    # Convert lowercased lines to char arrays
    # Filter out the non-letter characters from the array
    letters = lines.flatMap(lambda line: list(line.lower())).filter(lambda character: re.search(r"[a-z]",character) != None)

    ### Action on RDD
    letterCounts = letters.countByValue() # count words
    
    ### Output
    # output is sorted by key so it is easier to see the full alphabet
    for i in sorted (letterCounts) : 
        print("{} : {}".format(i, letterCounts[i]))
