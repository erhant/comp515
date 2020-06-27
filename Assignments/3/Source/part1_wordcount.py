'''
Part 1. 
Using Spark, write a program to count the number of words in books.txt.

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
    # split words
    # remove words with non word characters or numbers
    # remove length 1 words that are not 'a' or 'e'.
    words = lines.flatMap(lambda line: re.split(r" |\t|\'",line.lower())).filter(lambda word: (re.search(r"\W|\d",word) == None) and (re.search(r"\w",word) != None)).filter(lambda w: len(w) != 1 or (len(w) == 1 and (w == "a" or w == "e")) )

    ### Action
    wordCounts = words.countByValue() # count words
    
    ### Output RDD
    wordCounts = {k: v for k, v in sorted(wordCounts.items(), key=lambda item: item[1])}
    for word, count in wordCounts.items():
        while True:
            try:
                print("{} : {}".format(word, count))
                break
            except:
                word = word[1:] # usually there is a unicode character at the start that gives an encoding error
                # encoding this as a transformation is a problem

    print("Number of words:", len(wordCounts))
