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
 try:
  for i in range (0,len(data)):
   if(data[i][0]!=0):  
    sum+=data[i][1]*(1/data[i][0])
    div+=1/data[i][0]
  if (div!=0):
   return sum/div
  else: 
   return -1
 except ZeroDivision:
  print("error"+str(data))
 


def same_weigth(data):
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
 
