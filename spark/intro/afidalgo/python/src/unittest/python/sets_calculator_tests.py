from pyspark import SparkConf,SparkContext
from sets_calculator import SetMovies
import unittest


class TestCalculator (unittest.TestCase):

	def setUp(self):
	   conf = SparkConf().setAppName("appTest").setMaster("local[*]")
	   self.sc = SparkContext(conf=conf)
	   self.setMovies = SetMovies() 

	def tearDown(self):
	   self.sc.stop()

	def test_when_calculate_set_word_most_repeater(self):
	   entry = [('Toy', (2, ['::Toy Story Toy (1995)::'])),
	            ('ToyA', (3, ['::ToyA StoryA ToyA (1995)::'])),
	            ('Story', (1, ['::Toy Story Toy (1995)::'])),
	            ('StoryA', (3, ['::ToyA StoryA ToyA (1995)::']))]
	   result = [('ToyA', ['::ToyA StoryA ToyA (1995)::']),
	              ("StoryA",["::ToyA StoryA ToyA (1995)::"])]
	   funcReverseTuple = lambda value :(value[1][0],(value[0],value[1][1]))
	   rdd = self.sc.parallelize(entry)		              
	   self.assertEqual(self.setMovies.setWithMaxValues(rdd,funcReverseTuple),result)

	def test_when_calculate_maximum_year(self):
	   entry = [('(1996)',2),
	            ('(1998)',2),
	            ('(1997)',1)]  
	   result = ['(1996)','(1998)']
	   rdd = self.sc.parallelize(entry)	
	   self.assertEqual(self.setMovies.setWithMaxValuesB(rdd),result)         








