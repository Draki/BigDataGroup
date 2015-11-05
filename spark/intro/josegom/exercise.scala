 println("Wich is the year with more movies? (movies.dat)")

 val movies = sc.textFile("/home/jmgomez/projects/BigDataGroup/spark/intro/josegom/movies.dat")
 
 val years = movies.map(line => line.substring(line.indexOf('(')+1,line.indexOf(')')) ).filter(_ forall Character.isDigit).map(year => (year,1)).reduceByKey(_+_).sortBy(x =>x._2,false).take(1)
 
 println ("the year is " + years(0)._1)


 

 println( "Wich are the title film with the more common word? (word with more than three letters)(movies.dat)")


  val movies = sc.textFile("/home/jmgomez/projects/BigDataGroup/spark/intro/josegom/movies.dat")

val titles  = movies.map(line => {
	 val title = line.split("::")(1)
	 title.substring(0,title.indexOf('(')).replaceAll(",","").replaceAll("\\.","")

}
	)

val word = titles.flatMap(_.split(" ")).filter(_.size>3).map(x => (x,1)).reduceByKey(_+_).sortBy(x =>x._2,false).take(1)(0)._1

//Maybe  we can make it in other way if we can create a array like ((film1,word1)(film1FromFilm,word2FromFilm)(film2,word1FromFilm)......
titles.filter(_.contains(word)).collect

 
