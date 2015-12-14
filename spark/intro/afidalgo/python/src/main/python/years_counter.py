import re
from sets_calculator import SetMovies


class YearsCounter:

	 def getMaxValues(self,rdd):   
	    setMovies = SetMovies()
	    regexYearWithParenthesis = '[(][0-9][0-9][0-9][0-9][)]'	    
	    rdd =  rdd.map(lambda line :  (re.search(regexYearWithParenthesis,line).group(),1))
	    rdd =  rdd.reduceByKey(lambda firstValue,secondValue :(firstValue+secondValue))	
	    return setMovies.setWithMaxValues(rdd,lambda value :(value[1],value[0]))	    

