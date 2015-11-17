 println("Wich is the year with more movies? (movies.dat)")

 val movies = sc.textFile("/home/jmgomez/projects/BigDataGroup/spark/intro/josegom/movies.dat")
 
 val years = movies.map(line => line.substring(line.indexOf('(')+1,line.indexOf(')')) ).filter(_ forall Character.isDigit).map(year => (year,1)).reduceByKey(_+_).sortBy(x =>x._2,false).take(1)
 
 println ("the year is " + years(0)._1)


 

 println( "Wich are the title film with the more common word? (word with more than three letters)(movies.dat)")


  val movies = sc.textFile("/home/jmgomez/projects/BigDataGroup/spark/intro/josegom/movies.dat")
This is a solution in one line but worse because we are using groupBy and it feature have problem: Suffle and maybe a partition have more information that others. 
movies.map(line => {
      val title = line.split("::")(1)
      title.substring(0,title.indexOf('(')).replaceAll(",","").replaceAll("\\\\.","")
      }
      ).flatMap(x => x.split(" ").filter(_.size>3).map(y =>( y,x.dropRight(1)))).groupBy(x=>x._1).map(x=>(x._1,x._2.size,x._2)).max()(new Ordering[Tuple3[String, Int,Iterable[(String, String)]]]() {
        override def compare(x: (String, Int,Iterable[(String, String)]), y: (String, Int,Iterable[(String, String)])): Int = 
           Ordering[Int].compare(x._2, y._2)
      })._3.foreach(x=>println(x._1 + "<-->"+x._2))
      

 
