from pyspark import SparkContext
from pyspak.streaming import StreamingContext
import json
import csv

sc = SparkContext("Comment Streaming")
scc = StreamingContext(sc, 1)

comments = scc.textFileStream("./comments")
