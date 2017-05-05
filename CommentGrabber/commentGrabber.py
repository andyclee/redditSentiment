""""
FOR TRAINING DATA (SCORE):
This script automatically acquires comments
from reddit and writes them to text files
in a directory so they can be processed
by Spark Streaming

NOTE: This method does *not* guarantee that
comments will not be duplicated
Given the amount of activity on reddit though,
duplication is unlikely
"""

import praw
import time
import threading
import uuid
import sys
import re
import string
import os
import subprocess

reddit = praw.Reddit('bot1')

class commentQueue:
    def __init__(self):

        #Stores the list of comments to be written
        self.commentQ = []

    def pop(self):
        return self.commentQ.pop(0)

    def put(self, comments):
        return self.commentQ.append(comments)

    def getSize(self):
        return len(self.commentQ)

def getCommentIDs(commentQ):
    testLimit = 3
    currentIter = 0
    while currentIter < testLimit:
        print("Attempting to get recent comment IDs")
        try:
            requestWait = 2
            commentIDs = []
            for comment in reddit.subreddit('all').comments(limit=3):
                commentIDs.append(comment.id)
                time.sleep(requestWait)
            commentQ.put(commentIDs)
            print("Comment Queue size: " + commenQ.getSize())
            currentIter += 1
        except:
            currentIter += 1
            pass

def writeComments(commentQ):
    testLimit = 3
    currentIter = 0
    while currentIter < testLimit:
        try:
            writtenToTrain = 0
            trainLimit = 2
            
            comments = commentQ.pop()
            
            trainFilename = str(uuid.uuid4()) + ".txt"
            testFilename = str(uuid.uuid4()) + ".txt"
            
            trainCommentCSV = ""
            testCommentCSV = ""
            for commentID in comments:
                currentComment = reddit.comment(commentID)
                redditIsLazy = currentComment.body

                #Forces comment to evaluate
                print(redditIsLazy)
                commentInfo = vars(currentComment)
                row = createRow(commentInfo)

                if writtenToTrain < trainLimit:
                    trainCommentCSV += row
                    writtenToTrain += 1
                else:
                    testCommentCSV += row

            with open(os.path.join("./trainComments", trainFilename), 'w') as f:
                print("Writing to train file: " + trainFilename)
                f.write(trainCommentCSV)
                subprocess.call(["hdfs", "dfs", "-put", os.path.join("./trainComments", trainFilename), "/user/andylee/trainComments"])

            with open(os.path.join("./testComments", testFilename), 'w') as f:
                print("Writing to test file: " + testFilename)
                f.write(testCommentCSV)
                subprocess.call(["hdfs", "dfs", "-put", os.path.join("./testComments", testFilename), "/user/andylee/testComments"])

            currentIter += 1
        except IndexError:
            pass

def createRow(commentInfo):
    currentRow = ""
    currentRow += str(commentInfo['score']) + ","
    currentRow += stripPunctuation(commentInfo['body']) + ","
    currentRow += commentInfo['subreddit_id'] + ","
    currentRow += str(commentInfo['gilded']) + ","
    if commentInfo['distinguished'] == None:
        currentRow += ","
    else:
        currentRow += str(commentInfo['distinguished']) + ","
    currentRow += str(commentInfo['controversiality']) + "\n"

    return currentRow

def stripPunctuation(str_in):
    # Strip punctuation from word
    noPunc = re.sub('[%s]' % re.escape(string.punctuation), '', str_in)
    noNL = noPunc.replace('\n', '').replace('\r', '')
    return noNL

def collectAndWrite():
    commentQ = commentQueue()

    #86400 seconds in a day
    initWaitTime = 3

    idThread = threading.Thread(target=getCommentIDs, args=(commentQ,))
    idThread.start()

    time.sleep(initWaitTime)
    print("Wait time over, starting to write")
    
    writeThread = threading.Thread(target=writeComments, args=(commentQ,))
    writeThread.start()

collectAndWrite()
