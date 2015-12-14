import itertools

class SetMovies :

	def setWithMaxValues(self,rdd,funcReverseTuple):	
		rdd =  rdd.map(funcReverseTuple)
		rdd =  rdd.reduceByKey(lambda firstValue,secondValue : (firstValue,secondValue)).sortByKey(ascending=False)
		rdd =  rdd.map(lambda tuple : (tuple[1]))
		return rdd.first()


			







	    	