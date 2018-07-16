def mean(data,weigth):
 solution = {
	"Proximity":prox_weigth(data),
	"Same":same_weigth(data),
	"Linear":linear_weigth(data)
		 }
 return solution.get(weigth,"Wrong weigth")

 
def prox_weigth(data):
 sum=0
 div=0
 for i in range (0,len(data)):
  sum+=data[i][1]*(1/data[i][0])
  div+=1/data[i][0]
 return sum/div


def same_weigth(data):
 vector=[]
 sum=0
 div=0
 for i in range (0,len(data)):
  sum+=data[i][1]
  div+=1
 return sum/div 
 

def linear_weigth(data):
 sum=0
 div=0
 k=len(data)
 for i in range (0,len(data)):
  sum+=data[i][1]*(k-i)
  div+=(k-i)
 return sum/div
 
