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

def cleanText(text):
    wordList = []
    text = stripPunctuation(text.lower())
    rawWordList = word_tokenize(text)
    for word in rawWordList:
        if word not in stopwordsList:
            wordList.append(word)
    return wordList

def getDataScorePair(oneRow):
    body = oneRow[17]
    score = oneRow[15]
    cleanedText = cleanText(body)
    return (cleanedText, score)

redditData = sc.textFile("reddit.csv")
header = redditData.first()

redditData = redditData.filter(lambda x: x != header).map(parse_csv)

# index 17 is the column for body text
# length 22 is the length of one complete row, since it seems replace('\n')
# function works strangely when encounter non-english strings

textScorePair = redditData.filter(lambda x: len(x) == 22 and isEnglish(x[17])).map(lambda x: getDataScorePair(x))

# sample output
[(['gg', 'ones', 'watch', 'nfl', 'draft', 'guess'], '4'), (['really', 'implying', 'return', 'times', 'anywhere', 'near', 'political', 'environment', 'wont', 'much', 'luck', 'selling', 'american', 'people', 'governance', 'concept', 'without', 'ushering', 'american', 'revolution', '20'], '0'), (['one', 'european', 'accent', 'either', 'doesnt', 'exist', 'accents', 'europe', 'european', 'accent'], '3'), (['kid', 'reminds', 'kevin', 'sad'], '3'), (['haha', 'getting', 'nauseous', 'ingame', 'experience', 'would', 'given', 'whole', 'new', 'level', 'bloodborne'], '1'), (['lets', 'see', 'guys', 'side'], '2'), (['buy', 'mystery', 'sampler', 'small', 'batch', 'request'], '6'), (['nihilum', 'lg', 'significantly', 'better', 'theory', 'cant', 'really', 'think', 'replacement', 'ptr', 'would', 'leave', 'clg', 'better', 'place', 'cloud9', 'much', 'better', 'never', 'know'], '5'), (['fuck'], '4'), (['dont', 'diss', 'grim', 'puncher'], '1')]
