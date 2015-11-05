 println("Wich is the year with more movies? (movies.dat)")

 val movies = sc.textFile("/home/jmgomez/projects/BigDataGroup/spark/intro/josegom/movies.dat")
 
 val years = movies.map(line => line.substring(line.indexOf('(')+1,line.indexOf(')')) ).filter(_ forall Character.isDigit).map(year => (year,1)).reduceByKey(_+_).sortBy(x =>x._2,false).take(1)
 
 println ("the year is " + years(0)._1)