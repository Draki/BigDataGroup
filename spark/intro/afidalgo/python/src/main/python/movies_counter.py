import re
from years_counter import YearsCounter
from word_counter import WordCounter

class Counters:


	def year_with_more_movies(self,rdd):
		counter = YearsCounter()
		return counter.getMaxValues(rdd)
	 
	def word_more_repeater(self,rdd):
		wordCounter = WordCounter()
		return wordCounter.getMaxValues(rdd)










	    	


