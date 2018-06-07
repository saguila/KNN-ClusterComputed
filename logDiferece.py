import numpy as np
ftc=50  #variable global para eliminar flatlines

def deleteHeader(file):
#delete the header of the fileinput
        data = sc.textFile(file)
        header = data.first()
        data = data.filter(lambda x:x!= header)
        return data


def logarithmicPerformance(x):
        #Fuction to map the logarithmic performance on the RDDbitcoin file
                open=float(x[1])
                close=float(x[4])
                k=[x[0],np.log(close/open),open,close]
                return k

def removeFlatLine(x):
        global ftc
        if x[1]!=0:
                ftc=0
        else:
                ftc=ftc+1
        return ftc<50


RDD = deleteHeader('hdfs:///loudacre/kb/bigBT.csv').zipWithIndex().map(lambda(x,y):(y,x)).cache()
result = RDD.map(lambda (x,y):(x,logarithmicPerformance(y.split(",")))).filter(lambda (x,y):(x,removeFlatLine(y))).cache()
joined = result.join(RDD).map(lambda (x,y) : y)