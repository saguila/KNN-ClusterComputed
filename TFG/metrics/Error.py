'''
Calculate's the error between reality and prediction using error types.
@Param prediction value (pred), reality value (value), the type of error (err).
@Return an error value.
'''

def error(pred,value,err):
 solution = {
	"MAE":mean_absolute_error(pred,value),
	"ME":mean_error(pred,value),
	"RMSE":root_mean_squared_error(pred,value),
	"MAPE":mean_absolute_percentage_error(pred,value),
	"MPE":mean_percentage_error(pred,value)
		 }
 return solution.get(err,"Wrong Error")

 '''
Calculate's the error between reality and prediction using mean error (ME).
@Param prediction value (pred), reality value (value).
@Return a mean error value.
'''
 
def mean_error(pred,value):
 return (value-pred)

 
 '''
Calculate's the error between reality and prediction using mean absolute error (MAE).
@Param prediction value (pred), reality value (value).
@Return the mean absolute error value.
'''
def mean_absolute_error(pred,value):
 return abs(value-pred)


'''
Calculate's the error between reality and prediction using root mean squared error (RMSE).
@Param prediction value (pred), reality value (value).
@Return the root mean squared error value.
'''
 
def root_mean_squared_error(pred,value):
 return (value-pred)**2
 

 '''
Calculate's the error between reality and prediction using mean absolute percentage error (MAPE).
@Param prediction value (pred), reality value (value).
@Return the mean absolute percentage error value.
'''

def mean_absolute_percentage_error(pred,value):
	try:
		if(value!=0):
			return abs((value-pred)/value)
		else: 
			return 0
	except ZeroDivisionError:
		print("error MAPE")
		
		
		
 '''
Calculate's the error between reality and prediction using mean percentage error (MPE).
@Param prediction value (pred), reality value (value).
@Return the mean percentage error value.
'''
def mean_percentage_error(pred,value):
	try:
		if(value!=0):
			return (value-pred)/value
		else: 
			return 0
	except ZeroDivisionError:
		print("error MPE") 