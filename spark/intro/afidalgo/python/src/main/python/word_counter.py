import re
from sets_calculator import SetMovies

class WordCounter:


	def getMaxValues(self,rdd):
		setMovies = SetMovies()
		regexMoviesTitle = '::([a-z]|[A-Z]|[0-9]|[(]|[)]|[ ])*::'
		regexYear = '[(][0-9][0-9][0-9][0-9][)]'
		rdd =  rdd.map(lambda line :  re.search(regexMoviesTitle,line).group())
		rdd =  rdd.map(lambda movie : (movie,movie.replace('::','').split(" ")))
		rdd =  rdd.flatMap(lambda titleAndWords : map(lambda word: (word,(1,[titleAndWords[0]])),titleAndWords[1]))
		rdd =  rdd.filter(lambda wordsAndTitle : not re.match(regexYear, wordsAndTitle[0]) and len(wordsAndTitle[0])>=3)
		rdd =  rdd.reduceByKey(lambda firstValue,secondValue :(firstValue[0]+secondValue[0],list(set(firstValue[1]+secondValue[1]))))	
		return setMovies.setWithMaxValues(rdd,lambda value :(value[1][0],(value[0],value[1][1])))