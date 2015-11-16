# Spark First Steps Solutions

* Wich is the year with more movies?

val movies = sc.textFile("<PATH>/movies.dat")
val years =  movies.map(line => line.substring(line.indexOf('(')+1,line.indexOf(')')) ).filter(_ forall Character.isDigit).map(year => (year,1)).reduceByKey(_+_)
years.max()(new Ordering[Tuple2[String, Int]]() {
        override def compare(x: (String, Int), y: (String, Int)): Int = 
           Ordering[Int].compare(x._2, y._2)
      })

* Wich are the title film with the more common word? 

We have this solution:

val movies = sc.textFile("<PATH>/movies.dat")
val titles = movies.map(line => {
val title = line.split("::")(1)
title.substring(0,title.indexOf('(')).replaceAll(",","").replaceAll("\\\\.","")
}
)
val word = titles.flatMap(_.split(" ")).filter(_.size>3).map(x => (x,1)).reduceByKey(_+_).sortBy(x =>x._2,false).take(1)(0)._1
titles.filter(_.contains(word)).collect

But we are thinking about solve this problem with only one val. At now we have:
val movies = sc.textFile("<PATH>/movies.dat")

val titles = movies.map(line => {
      val title = line.split("::")(1)
      title.substring(0,title.indexOf('(')).replaceAll(",","").replaceAll("\\\\.","")
      }
      ).flatMap(x => x.split(" ").map(y =>( y,x,1)))



