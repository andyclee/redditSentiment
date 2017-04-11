from pyspark import SparkContext, SparkConf
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.regression import LinearRegressionWithSGD
from pyspark.mllib.regression import RidgeRegressionWithSGD
from pyspark.mllib.regression import LassoWithSGD
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.mllib.linalg import SparseVector
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import csv
import string
import re
import numpy as np
conf = SparkConf().setAppName("NLP on Reddit Data")
sc = SparkContext(conf=conf)

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
Scaled the distinguished to: "" -> 0, "moderator" -> 20, "admin" -> 30, "other" -> 10
Scaled gilded to gilded * 10
Scaled the controversiality to controversiality * 10
"""
def getDataScorePair(oneRow):
    considered = []
    subredditId = oneRow[2]
    gilded = oneRow[11]
    distinguished = oneRow[-4]
    if distinguished == '':
        distinguished = 0
    elif distinguished == 'moderator':
        distinguished = 20
    elif distinguished == 'admin':
        distinguished = 30
    else:
        distinguished = 10
    controversiality = oneRow[-2]
    body = oneRow[17]
    cleanedBody = cleanText(body)

    considered.append(cleanedBody)
    considered.append(subredditId)
    considered.append(int(gilded) * 10)
    considered.append(distinguished)
    considered.append(int(controversiality)*10)
    score = oneRow[15]

    return (considered, score)

"""
add controversiality, gilded, and distinguished to the end of computed tfidf
"""
def combineFeatures(x):

    # x[1] is the computed tfidf sparse vector
    idxes = x[1].indices
    values = x[1].values
    length = x[1].size

    # append features
    idxes = np.append(idxes, length)
    idxes = np.append(idxes, length+1)
    idxes = np.append(idxes, length+2)
    values = np.append(values, x[0][0])
    values = np.append(values, x[0][1])
    values = np.append(values, x[0][2])
    return SparseVector(length + 3, idxes, values)

redditData = sc.textFile("/user/jl28/reddit.csv")
redditData = sc.parallelize(redditData.take(10000))
header = redditData.first()

# parse csv input into rows of list
redditData = redditData.filter(lambda x: x != header).map(parse_csv)

"""
    index 17 is the column for body text
    length 22 is the length of one complete row, since it seems replace('\n')
    function works strangely when encounter non-english strings
"""
textScorePair = redditData.filter(lambda x: len(x) == 22 and isEnglish(x[17])).map(lambda x: getDataScorePair(x)).filter(lambda x: isInt(x[1]))

texts = textScorePair.map(lambda x:x[0][0])
scores = textScorePair.map(lambda x: x[1])

# perform tf-idf on texts
tf = HashingTF().transform(texts)
idf = IDF(minDocFreq=5).fit(tf)
tfidf = idf.transform(tf)

otherFeatures = textScorePair.map(lambda x: [x[0][2],x[0][3],x[0][4]])
combinedFeatures = features.zip(tfidf).map(lambda x: combineFeatures(x))

zipped_data = (scores.zip(newFeatures)
                     .map(lambda x: LabeledPoint(x[0], x[1]))
                     .cache())

# Do a random split so we can test our model on non-trained data
training, test = zipped_data.randomSplit([0.7, 0.3])

# Train our model
model = LinearRegressionWithSGD.train(training)

# Use our model to predict
train_preds = (training.map(lambda x: x.label)
                       .zip(model.predict(training.map(lambda x: x.features))))
test_preds = (test.map(lambda x: x.label)
                  .zip(model.predict(test.map(lambda x: x.features))))

# Ask PySpark for some metrics on how our model predictions performed
trained_metrics = RegressionMetrics(train_preds.map(lambda x: (float(x[1]),x[0])))
test_metrics = RegressionMetrics(test_preds.map(lambda x: (float(x[1]),x[0])))

with open('result.txt', 'w+') as f:
    f.write(str(trained_metrics.explainedVariance) + '\n')
    f.write(str(trained_metrics.rootMeanSquaredError) + '\n')
    f.write(str(test_metrics.explainedVariance) + '\n')
    f.write(str(test_metrics.rootMeanSquaredError) + '\n')
