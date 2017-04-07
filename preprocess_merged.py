from pyspark import SparkContext, SparkConf
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import csv
import string
import re
conf = SparkConf().setAppName("NLP on Reddit Data")
sc = SparkContext(conf=conf)

stopwordsList = stopwords.words('english')

def parse_csv(x):
    x = x.replace('\n', '')
    d = csv.reader([x])
    return next(d)

# -*- coding: utf-8 -*-
def isEnglish(text):
    print (text)
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

def getDataScorePair(oneRow):
    considered = []
    subredditId = oneRow[2]
    gilded = oneRow[11]
    distinguished = oneRow[-4]
    controversiality = oneRow[-2]
    body = oneRow[17]
    cleanedBody = cleanText(body)

    considered.append(cleanedBody)
    considered.append(subredditId)
    considered.append(gilded)
    considered.append(distinguished)
    considered.append(controversiality)
    score = oneRow[15]

    return (considered, score)

redditData = sc.textFile("reddit.csv")
header = redditData.first()

# parse csv input into rows of list
redditData = redditData.filter(lambda x: x != header).map(parse_csv)

"""
    index 17 is the column for body text
    length 22 is the length of one complete row, since it seems replace('\n')
    function works strangely when encounter non-english strings
"""
textScorePair = redditData.filter(lambda x: len(x) == 22 and isEnglish(x[17])).map(lambda x: getDataScorePair(x))
