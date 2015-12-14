from pyspark import SparkConf,SparkContext
from movies_counter import Counters
import unittest


class TestCounters(unittest.TestCase):

	def setUp(self):
	   conf = SparkConf().setAppName("appTest").setMaster("local[*]")
	   self.sc = SparkContext(conf=conf)
	   self.counter = Counters() 

	def tearDown(self):
	   self.sc.stop()

	def test_when_exist_one_movie_in_rdd(self):
		movieList = ["1::Toy Story (1995)::Animation|Children's|Comedy",]
		movies = self.sc.parallelize(movieList)
		self.assertEqual(self.counter.year_with_more_movies(movies),('(1995)'))

	def test_when_exist_three_movies_in_rdd(self):
		movieList = ["1993::Toy Story (1995)::Animation|Children's|Comedy",
		             "1993::Toy Story (1996)::Animation|Children's|Comedy",
		             "1993::Toy Story (1996)::Animation|Children's|Comedy"]
		movies = self.sc.parallelize(movieList)
		self.assertEqual(self.counter.year_with_more_movies(movies),('(1996)'))

	def test_when_exist_one_movie_and_counter(self):
	   movieList = ["1993::Toy Story Toy (1995)::Animation|Children's|Comedy",
	                "1993::ToyA StoryA ToyA (1995)::Animation|Children's|Comedy"]	 
	   result = (('ToyA', ['::ToyA StoryA ToyA (1995)::']),
	             ('Toy', ['::Toy Story Toy (1995)::']))                 
	   movies = self.sc.parallelize(movieList)
 	   self.assertEqual(self.counter.word_more_repeater(movies),result)
