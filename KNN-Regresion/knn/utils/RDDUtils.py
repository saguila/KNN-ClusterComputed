#Remove the string header from the RDD
#@Return an RDD without string header
def deleteHeader(idx, iter):
    output=[]
    for sublist in iter:
        output.append(sublist)
    if idx>0:
        return(output)
    else:
        return(output[1:])


def convertData(x):
	if isinstance(x, list):
		for i in range (0,len(x)):
			x[i]=float(x[i])
		return x
	else:
		return [float(x)]

def reduction(a,b,k):
	out=[]
	if isinstance(a[0], list):
		for i in range(0,len(a)):
			out.append(a[i])
	else:
		out.append(a)
	if isinstance(b[0], list):
		for i in range(0,len(b)):
			out.append(b[i])
	else:
		out.append(b)
	out=sorted(out)
	return out[:k]