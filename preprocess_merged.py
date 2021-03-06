from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
from pyspark.sql import HiveContext
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import RidgeRegressionWithSGD
from pyspark.mllib.regression import LassoWithSGD
from pyspark.mllib.tree import DecisionTree
from pyspark.mllib.tree import RandomForest
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.mllib.tree import GradientBoostedTrees
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField
from pyspark.sql import Row
from pyspark.sql.window import Window
from pyspark.sql.functions import rowNumber
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from pyspark.mllib.linalg import VectorUDT
import csv
import string
import re
import numpy as np
from pyspark.mllib.stat import Statistics
conf = SparkConf().setAppName("NLP on Reddit Data")
conf.set("spark.driver.maxResultSize", "3g")
sc = SparkContext(conf=conf)

# notice here we use HiveContext(sc) because window functions require HiveContext
sqlContext = HiveContext(sc)
stopwordsList = stopwords.words('english')

def parse_csv(x):
    x = x.replace('\n', '')
    d = csv.reader([x])
    return next(d)

def isInt(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

# -*- coding: utf-8 -*-
def isEnglish(text):
    try:
        text.encode('ascii')
    except UnicodeEncodeError:
        return False
    else:
        return True

def stripPunctuation(str_in):
    # Strip punctuation from word
    return re.sub('[%s]' % re.escape(string.punctuation), '', str_in)

def reachSampleLimit(score):
    if score in frequencyCount:
        if frequencyCount[score] > 300:
            return True
        else:
            frequencyCount[score] += 1
            return False
    else:
        frequencyCount[score] = 1
        return False

"""
Clean the comment body text
"""
def cleanText(text):
    wordList = []
    text = stripPunctuation(text.lower())
    rawWordList = word_tokenize(text)
    for word in rawWordList:
        if word not in stopwordsList:
            wordList.append(word)
    return wordList

"""
Considered data in following order:
Text, Subreddit ID, Number of gilds, Distinguished, Controversiality
Scaled the distinguished to: "" -> 0, "moderator" -> 2, "admin" -> 3, "other" -> 1
Scaled gilded to gilded * 10
"""
def getDataScorePair(oneRow):
    considered = []
    subredditId = oneRow[2]
    gilded = oneRow[11]
    distinguished = oneRow[-4]
    if distinguished == '':
        distinguished = 0
    elif distinguished == 'moderator':
        distinguished = 2
    elif distinguished == 'admin':
        distinguished = 3
    else:
        distinguished = 1
    controversiality = oneRow[-2]
    body = oneRow[17]
    cleanedBody = cleanText(body)
    considered.append(cleanedBody)
    considered.append(subredditId)
    considered.append(int(gilded) * 10)
    considered.append(distinguished)
    considered.append(int(controversiality))
    score = int(oneRow[15])

    return (considered, score)

redditData = sc.textFile("/user/jl28/reddit.csv", 500)
redditData = sc.parallelize(redditData.take(400000), 500)
header = redditData.first()

# parse csv input into rows of list
redditData = redditData.filter(lambda x: x != header).map(parse_csv)

"""
    index 17 is the column for body text
    length 22 is the length of one complete row, since it seems replace('\n')
    function works strangely when encounter non-english strings
"""
# Drop non-english rows and ensure the result data is not malformed, then get data score pair and ensure the score is invalid
textScorePair = redditData.filter(lambda x: len(x) == 22 and isEnglish(x[17]) and isInt(x[11]) and isInt(x[-2]) and isInt(x[15])).map(lambda x: getDataScorePair(x)).filter(lambda x: (x[1] < 100 and x[1] > -100)).collect()

frequencyCount = {}
samplesFiltered = []
for item in textScorePair:
    if (not reachSampleLimit(item[1])):
        samplesFiltered.append(item)

print (frequencyCount)
textScorePair = sc.parallelize(samplesFiltered,500)

# perform tf-idf on texts
texts = textScorePair.map(lambda x:x[0][0])
tf = HashingTF().transform(texts)
idf = IDF(minDocFreq=5).fit(tf)
tfidf = idf.transform(tf)

# build dataframe with column shown in schema, the reason to build data frame is that VectorAssembler's input should be two column in dataframe
schema = 'score,gilded,distinguished,controversiality'.split(',')
itemsForDataFrame = textScorePair.map(lambda x: [x[1], x[0][2], x[0][3], x[0][4]])
otherFeaturesDF = sqlContext.createDataFrame(itemsForDataFrame, schema)

# build datafame for tf_idf, same reason as above
tfidfSchema = StructType([StructField("tf_idf", VectorUDT(), True)])
row = Row("tf_idf")
tfidfDF = tfidf.map(lambda x: row(x)).toDF(tfidfSchema)

# add row number to the two dataframe, in order to perform a join
w = Window().orderBy()
otherFeaturesDF =  otherFeaturesDF.withColumn("columnindex", rowNumber().over(w))
tfidfDF =  tfidfDF.withColumn("columnindex", rowNumber().over(w))

mergedDF = otherFeaturesDF.join(tfidfDF, otherFeaturesDF.columnindex == tfidfDF.columnindex, 'inner')

# assemble the tf-idf and other features to form a single vector
assembler = VectorAssembler(inputCols=["tf_idf", "gilded", "distinguished", "controversiality"],outputCol="features")
mergedDF = assembler.transform(mergedDF)
mergedDF.show()
scoreFeaturesPair = mergedDF.map(lambda x: (x[7],x[0])).repartition(500)
features = scoreFeaturesPair.map(lambda x: x[0])
scores = scoreFeaturesPair.map(lambda x: int(x[1]))

zipped_data = (scores.zip(features)
                     .map(lambda x: LabeledPoint(x[0], x[1]))
                     .cache())

# Do a random split so we can test our model on non-trained data
training, test = zipped_data.randomSplit([0.7, 0.3])

# Train our model
model = RandomForest.trainRegressor(training, {1048577: 4, 1048578: 2},10)
#model = LinearRegressionWithSGD.train(training)

# Use our model to predict
train_preds = (training.map(lambda x: x.label)
                       .zip(model.predict(training.map(lambda x: x.features))))
test_preds = (test.map(lambda x: x.label)
                  .zip(model.predict(test.map(lambda x: x.features))))

# Ask PySpark for some metrics on how our model predictions performed
trained_metrics = RegressionMetrics(train_preds.map(lambda x: (float(x[1]),x[0])))
test_metrics = RegressionMetrics(test_preds.map(lambda x: (float(x[1]),x[0])))

with open('reSampleResult2.txt', 'w+') as f:
    f.write(str(trained_metrics.explainedVariance) + '\n')
    f.write(str(trained_metrics.rootMeanSquaredError) + '\n')
    f.write(str(trained_metrics.r2) + '\n')
    f.write(str(test_metrics.explainedVariance) + '\n')
    f.write(str(test_metrics.rootMeanSquaredError) + '\n')
    f.write(str(test_metrics.r2) + '\n')
    f.write(str(frequencyCount) + '\n')
