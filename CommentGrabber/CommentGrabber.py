""""
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
from pprint import pprint

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
            for comment in reddit.subreddit('all').comments(limit=100):
                commentIDs.append(comment.id)
                commentQ.put(commentIDs)
                time.sleep(requestWait)

            currentIter += 1
        except:
            currentIter += 1
            pass

def writeComments(commentQ):
    testLimit = 3
    currentIter = 0
    while currentIter < testLimit:
        print("Attempting to write comment to file")
        try:
            comments = commentQ.pop()
            filename = str(uuid.uuid4()) + ".txt"
            commentCSV = ""
            for commentID in comments:
                currentComment = reddit.comment(commentID)
                commentInfo = vars(currentComment)
                row = createRow(commentInfo)
                commentCSV += row

            with open(filename, 'w') as f:
                f.write(commentCSV)

            currentIter += 1
        except:
            currentIter += 1
            pass

def createRow(commentInfo):
    currentRow = ""
    currentRow += stripPunctuation(commentInfo['body']) + ","
    currentRow += commentInfo['subreddit_id'] + ","
    currentRow += commentInfo['gilded'] + ","
    if commentInfo['distinguished'] == None:
        currentRow += ","
    else:
        currentRow += commentInfo['distinguished'] + ","
    currentRow += commmentInfo['controversiality'] + "\n"

    return currentRow

def stripPunctuation(str_in):
    # Strip punctuation from word
    return re.sub('[%s]' % re.escape(string.punctuation), '', str_in)

def collectAndWrite():
    commentQ = commentQueue()
    
    initWaitTime = 5

    idThread = threading.Thread(target=getCommentIDs, args=(commentQ,))
    idThread.start()

    time.sleep(initWaitTime)
    print("Wait time over, starting to write")
    
    writeThread = threading.Thread(target=writeComments, args=(commentQ,))
    writeThread.start()

collectAndWrite()
