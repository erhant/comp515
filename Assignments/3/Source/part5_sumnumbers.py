'''
Part 5. 
Using Spark, compute the sum of numbers given in the numbers.txt

I made this program so that we can launch it in two ways:
$spark-submit part5_sumnumbers.py [1|2|4|8|16|32] --> this will run on a single file only, specified by the command line argument.
$spark-submit part5_sumnumbers.py --> without command line argument, it runs on all files and plots the bar graph.

Run using: $spark-submit 
'''

# run this file with spark-submit.cmd
import re # regex for words
import sys # to get argument
import time # to have a stopwatch
import matplotlib.pyplot as plt # to plot
from pyspark import SparkContext, SparkConf

numChoices = [1, 2, 4, 8, 16, 32]
num = -1
if len(sys.argv) > 1:
    num = int(sys.argv[1])
if num != -1:
    # Custom input
    try:
        numChoices.index(num)
    except:
        num = 1 # it will get here if argument is not valid

    filename = "numbers"
    if num != 1:
        filename += str(num)
    filename += ".txt"
    filenamelist = [filename]
else:
    # Do all of them
    filenamelist = ["numbers.txt", "numbers2.txt", "numbers4.txt", "numbers8.txt", "numbers16.txt", "numbers32.txt"]

### MAIN ###    
if __name__ == "__main__":

    ### Create Spark Context
    conf = SparkConf().setAppName("Part1").setMaster("local[*]") # use all available cores
    sc = SparkContext(conf = conf)
    
    sumVals = []
    computeTimes = [] # (ms)
    fileSizes = ["22582", "45163", "90329", "180656", "361309", "722614"] # (KB)

    for f in filenamelist:
        ### Load RDD
        lines = sc.textFile("./in/numbers/"+f) # get the article
        
        ### Transform on RDD
        # split strings, remove empty ones if there is any, convert to numbers
        numbers = lines.flatMap(lambda line: line.split(" ")).filter(lambda s: s).map(lambda n: float(n))

        ### Action on RDD
        now = time.time()
        sumOfNumbers = numbers.reduce(lambda x, y: x + y)
        future = time.time()

        ### Output
        print("Sum of numbers in {} is {}\nIt took spark {} milliseconds to compute that.\n".format(f, sumOfNumbers,future-now))
        sumVals.append(sumOfNumbers)
        computeTimes.append(future-now)

    if num == -1:
        x_pos = [i for i, _ in enumerate(fileSizes)]
        ### Plot
        plt.bar(x_pos, computeTimes, color='green')
        plt.xlabel("File Size (KB)")
        plt.ylabel("Compute Time (ms)")
        plt.title("Compute Time with respect to File Size")
        plt.xticks(x_pos, fileSizes)
        plt.savefig('part5_plot.png')
        plt.show()        
        print("Plot saved to active directory as: part5_plot.png")
    else:
        print("Not plotting anything for a single file.")

