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


* This is a solution in one line but worse because we are using groupBy and it feature have problem: Suffle and maybe a partition have more information that others.

movies.map(line => {
val title = line.split("::")(1)
title.substring(0,title.indexOf('(')).replaceAll(",","").replaceAll("\\\\.","")
}
).flatMap(x => x.split(" ").filter(_.size>3).map(y =>( y,x.dropRight(1)))).groupBy(x=>x._1).map(x=>(x._1,x._2.size,x._2)).max()(new Ordering[Tuple3[String, Int,Iterable[(String, String)]]]() {
override def compare(x: (String, Int,Iterable[(String, String)]), y: (String, Int,Iterable[(String, String)])): Int =
Ordering[Int].compare(x._2, y._2)
})._3.foreach(x=>println(x._1 + "<-->"+x._2))


* This is a solution with two val but maybe better
* 

val titles = movies.map(line => {
val title = line.split("::")(1)
title.substring(0,title.indexOf('(')).replaceAll(",","").replaceAll("\\.","")
}
)
val word = titles.flatMap(_.split(" ")).filter(_.size>3).map(x => (x,1)).reduceByKey(_+_).sortBy(x =>x._2,false).take(1)(0)._1
//Maybe we can make it in other way if we can create a array like ((film1,word1)(film1FromFilm,word2FromFilm)(film2,word1FromFilm)......
titles.filter(_.contains(word)).collect




It is interesting to study what happens if a film has the same word two or more times, for example

Love<-->I Love You I Love You Not





