from pyspark.mllib.classification import LogisticRegressionModel
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark.mllib.evaluation import RegressionMetrics, RankingMetrics
from nltk.tokenize import word_tokenize
import string
import csv
from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("Reddit NLP Sentiment Static")
sc = SparkContext(conf=conf)

"""
We get a tuple of the score, an array of the relevant param values
The parameters collected are as follows:
subreddit_id, gilded, body, distinguished, controversiality 
"""
def getTextAndParams(comment):
    considered = []
    sub = comment[2]
    gilded = comment[11]
    body = comment[17]
    dist = comment[-4]
    contr = comment[-2]
    
    considered.append(sub)
    considered.append(gilded)
    considered.append(body)
    considered.append(dist)
    considered.append(contr)

    score = comment[15]

    return score, considered

"""
Make sure comment contains all proper fields
and that fields are of expected data type
"""
def validateComment(comment):
    score = comment[0]
    params = comment[1]

    try:        
        score = int(score)
        sub = int(params[0])
        gilded = int(params[1])
        body = str(params[2])
        dist = int(params[3])
        contr = int(params[4])

        if ( (gilded != 0) and (gilded != 1) ):
            return False
        if ( (dist != 0) and (dist != 1) ):
            return False
        if ( (contr != 0) and (contr != 1) ):
            return False

        return True
    
    except:
        return False

def tokenizeNoStopText(text):
    textTokenized = word_tokenize(text)
    textSet = set(textTokenized)
    stopWords = set(['why', 'who', 'over', 'my', 'yours', 'out', 'him', 'how', 'haven', 'don', 'same', 'he', 'yourselves', 'by', 'so', 'be', 'm', 'own', 'any', 'from', 'myself', 'am', 'then', 'than', 'under', 'because', 'have', 'both', 'such', 'very', 'didn', 'doing', 'after', 'ain', 'it', 'each', 'for', 'or', 'our', 'mustn', 'those', 'shouldn', 'ourselves', 't', 'is', 'if', 'at', 'more', 'aren', 'did', 'few', 'the', 'you', 'about', 'where', 'hadn', 'below', 'these', 'are', 've', 'itself', 'this', 'here', 'with', 'themselves', 'whom', 'me', 'they', 'wasn', 'i', 'been', 'down', 'weren', 'being', 'a', 'now', 'before', 'was', 'hasn', 'ours', 'off', 'no', 'do', 'we', 'as', 'on', 'too', 'when', 'through', 'will', 'hers', 's', 'should', 'to', 'has', 'all', 'above', 'were', 'but', 'does', 'y', 'while', 'her', 'again', 'just', 'against', 'there', 'wouldn', 'o', 'further', 'your', 'their', 'herself', 're', 'mightn', 'his', 'up', 'its', 'she', 'isn', 'once', 'himself', 'between', 'won', 'what', 'couldn', 'that', 'not', 'of', 'during', 'shan', 'other', 'nor', 'd', 'yourself', 'them', 'which', 'and', 'some', 'needn', 'having', 'only', 'can', 'doesn', 'll', 'in', 'most', 'until', 'into', 'had', 'theirs', 'ma', 'an'])
    punc = list(string.punctuation)
    punc.append("''")
    punc.append("``")
    punc.append("<br \>")
    punc = set(punc)

    textFinal = textSet.difference(stopWords)
    textFinal = textFinal.difference(punc)

    return list(textFinal)

"""
Sets data into proper types
Prepares raw data from regression
"""
def prepareData(comment):
    score = int(comment[0])
    params = comment[1]
    params[0] = int(params[0])
    params[1] = int(params[1])
    params[2] = str(params[2])
    params[3] = int(params[3])
    params[4] = int(params[4])

    return score, params

#Load dataset
comments = sc.textFile("hdfs:///user/andylee/reddit.csv", minPartitions=100)

#Extract then remove the header
header = comments.first()
comRDD = comRDD.filter(lambda x : x != header)

#Just process 100 for now
comments = sc.parallelize(comments.takeSample(True, 100), 100)

comRDD = comRDD.mapPartitions(lambda com : csv.reader(com))
comRDD = comRDD.map(lambda com : getTextAndParams(com))
comRDD = comRDD.map(lambda com : validateComment(com))
comRDD = comRDD.map(lambda com : prepareData(com))
