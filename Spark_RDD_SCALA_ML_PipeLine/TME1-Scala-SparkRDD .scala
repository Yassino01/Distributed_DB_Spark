// Databricks notebook source
// MAGIC %md
// MAGIC * Author: Mohamed-Amine Baazizi
// MAGIC * Affiliation: LIP6 - Faculté des Sciences - Sorbonne Université
// MAGIC * Email: mohamed-amine.baazizi@lip6.fr

// COMMAND ----------

// MAGIC %md
// MAGIC # Prise en main de Scala (30 mn)

// COMMAND ----------

// MAGIC %md
// MAGIC Cet exercice illustre les différentes structures de contrôle de Scala présentées en cours. 
// MAGIC Il permet de comprendre le paradigme fonctionnel : seules les fonctions `map, reduce, flatten, filter, flatMap` sont autorisées.
// MAGIC 
// MAGIC Temps consillé : 30 mn

// COMMAND ----------

// MAGIC %md
// MAGIC ## Question 1

// COMMAND ----------

// MAGIC %md
// MAGIC Définir trois fonctions qui prennent en entrée une liste d'entiers `liste` et réalisent les opérations suivantes:
// MAGIC * `maxEntiers` retourne le plus grand des entiers de `liste` 
// MAGIC * `scEntiers` retourne la somme des carrés des entiers de `liste`
// MAGIC * `moyEntiers` retourne la moyenne des entiers de `liste`
// MAGIC 
// MAGIC Tester votre réponses en invoquant ces fonctions sur `listeEntiers` définie comme suit

// COMMAND ----------

val listeEntiers = List.range(1,11)

// COMMAND ----------

def maxEntiers(in: List[Int]) : Int = in.reduce((x, y) => x max y)   //complete here
def scEntiers(in: List[Int]) : Int = in.map(x => x * x).reduce((x, y) => x + y) //complete here
def moyEntiers(in: List[Int]) : Float = {in.reduce((x, y) => x + y).toFloat / in.map(x => 1).reduce((x, y) => x + y)}

// COMMAND ----------

moyEntiers(listeEntiers)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Question 2

// COMMAND ----------

// MAGIC %md
// MAGIC Soit une liste chaine de caractères construite à l'aide de l'instruction suivante.

// COMMAND ----------

val listeTemp = List("7,2010,04,27,75", "12,2009,01,31,78", "41,2009,03,25,95", "2,2008,04,28,76", "7,2010,02,32,91")


// COMMAND ----------

// MAGIC %md
// MAGIC Chaque élément représente un enregistrement fictif de températures avec le format (station, année, mois, température, code_département). On voudrais calculer, pour l'année 2009, le maximum et la moyenne de ses températures.
// MAGIC Compléter l'instruction suivante qui permet les transformations et les conversions de type nécessaires pour l'évaluation de ces deux calculs. 

// COMMAND ----------

val temp2009 = listeTemp.map(x=>x.split(",")).filter(x => x(1).toInt==2009).map(x=>x(3).toInt)
//
println(temp2009)
println("max des temps " + maxEntiers(temp2009))
println("moy des temps " + moyEntiers(temp2009))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Question 3

// COMMAND ----------

// MAGIC %md
// MAGIC Soit une liste chaine de caractères construite à l'aide de l'instruction suivante.

// COMMAND ----------

val melange = List("1233,100,3,20171010", "1224,22,4,20171009", "100,lala,comedie", "22,loup,documentaire")

// COMMAND ----------

// MAGIC %md
// MAGIC Deux types d'éléments existent : ceux de la forme (userID, movieID, rating, timestamp) et ceux de la forme (movieID, title, genre). Le domaine des userID est [1000, 2000] et celui des movieID est [0, 100].
// MAGIC 
// MAGIC Il est demandé de construire à partir de `melange` deux listes distinctes :
// MAGIC 
// MAGIC * `notes` contenant les éléments de la forme (userID, movieID, rating, timestamp) et dont le type est (Int, String, Int, Int) 
// MAGIC * `films` contenant les éléments de la forme (movieID, title, genre) et dont le type est (Int, String, String).

// COMMAND ----------

val notes = melange.map(x=>x.split(",")).filter(x => x.map(x => 1).reduce((x, y) => x + y) == 4).map(x => (x(0).toInt, x(1).toString, x(2).toInt, x(3).toInt))//complete here
println(notes)
val films = films.//complete here
println(films)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Question 4

// COMMAND ----------

// MAGIC %md
// MAGIC Soit une liste personnes contenant des tuples ayant trois attributs décrivant des personnes :
// MAGIC 
// MAGIC * le premier attribut est le nom de la personne;
// MAGIC * le second attribut est le type de la personne : etudiant (etu), enseignant (ens) ou inconnu (nan);
// MAGIC * la troisième attribut est l'année d'inscription (s'il s'agit d'un étudiant) ou les années d'ancienneté pour les enseignants.

// COMMAND ----------

val personnes = List(("Joe", "etu", 3), ("Lee", "etu", 4), ("Sara", "ens", 10), ("John", "ens", 5), ("Bill", "nan",20))

// COMMAND ----------

// MAGIC %md
// MAGIC Définir une classe `Etu(nom:String, annee:Int)` et une classe `Ens(nom:String, annee:Int)`
// MAGIC 
// MAGIC Transformer la liste `personnes` en une liste d'objets de la classe `Etu` ou `Ens` encapsulant les information des tuples en entrée. Par exemple, le tuple `("Joe", "etu", 3)` devra être transformé en un objet `Etu("Joe", 3)`.
// MAGIC 
// MAGIC Attention Les personnes de type inconnu ne doivent être dans le résultat!
// MAGIC 
// MAGIC Utiliser le pattern matching pour répondre à cette question.

// COMMAND ----------

class Etu(nom:String, annee:Int) //complete here 
class Ens(nom:String, annee:Int) //complete here
val classes_personnes = personnes.map({case (name, type_person, annee) => type_person match {
  case "etu" => new Etu(annee, annee)
  case "ens" => new Ens(annee, annee)
  case _ =>
}}).filter(x =>) //complete here

println(classes_personnes)

// COMMAND ----------

// MAGIC %md
// MAGIC # Prise en main de Spark RDD

// COMMAND ----------

// MAGIC %md
// MAGIC Prendre le temps de lire la documentation Spark sur l'utilisation des RDD
// MAGIC https://spark.apache.org/docs/latest/rdd-programming-guide.html
// MAGIC 
// MAGIC et sur l'API RDD
// MAGIC https://spark.apache.org/docs/2.1.1/api/scala/index.html#org.apache.spark.rdd.RDD

// COMMAND ----------

// MAGIC %md
// MAGIC ## Préparation datasets

// COMMAND ----------

// MAGIC %md
// MAGIC Télécharger en local les archives 
// MAGIC *  https://nuage.lip6.fr/s/Rg79eptSmBL3L2c 
// MAGIC et
// MAGIC * https://nuage.lip6.fr/s/E3ZxN9kMgFDZLDS
// MAGIC 
// MAGIC Déarchiver chacune des archives
// MAGIC * wordcount.txt.bz2
// MAGIC and
// MAGIC * Books.zip
// MAGIC 
// MAGIC puis charger les fichiers `wordcount.txt`, `books.csv`, `ratings.csv`	et `users.csv` dans le répertoire `/FileStore/tables/BDLE/TME1/` de Databricks

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercice 1 (30 mn) 

// COMMAND ----------

// MAGIC %md
// MAGIC Cet exercice utilise le dataset `wordcount`
// MAGIC 
// MAGIC Exécuter la commande suivante pour le charger dans la valeur `data` et tester que le chargement marche bien en lisant les 10 premieres lignes. 

// COMMAND ----------

val path = "/FileStore/tables/BDLE/TME1/" 
val data = sc.textFile(path + "wordcount.txt")

data.take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC *Remarque* pour partitionner une chaîne de caractères en utilisant le point (.) comme délimiteur à l'aide de la méthode `split()`, il faut protéger le point avec deux backslahs comme suit `split("\\.")`

// COMMAND ----------

// MAGIC %md
// MAGIC #### Q1. Structurer le contenu de data de sorte à obtenir un tableau de tableaux de chaines de caractères. Ce dernier devra être stocké dans une nouvelle variable nommée list.

// COMMAND ----------

val list = data.map(x => x.split(" "))//complete here

list.take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Q2. Afficher les 100 premiers éléments de la 3e colonne de `list`.

// COMMAND ----------

val q2 = list.map(x => x(2))//complete here
q2.take(100)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Q3. Transformer le contenu de `list` en une liste de paires `(mot, nb)` où `mot` correspond à la première colonne de `list` et `nb` sa troisième colonne.

// COMMAND ----------

val q3 = list.map(x => (x(0), x(2).toInt))//complete here
q3.take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Q4. Grouper les paires par l'attribut `mot` et additionner leur nombres respectifs.

// COMMAND ----------

val q4 = q3.reduceByKey((x, y) => x + y)//complete here
q4.collect

// COMMAND ----------

// MAGIC %md
// MAGIC #### Q5. Reprendre les questions Q3 et Q4 en calculant `mot` différemment : 
// MAGIC 
// MAGIC désormais, `mot` doit correspondre au préfixe du premier sous-élément de chaque élément de `list`, 
// MAGIC 
// MAGIC Exemple
// MAGIC * pour `en.d`, mot doit être `en`
// MAGIC * pour `fr.d`, mot doit être `fr`, etc. 
// MAGIC 
// MAGIC Comparer les résultats avec ceux obtenus précédemment.

// COMMAND ----------

val q5 = list.map(x => (x(0).split("\\.")(0), x(2).toInt)).reduceByKey((x, y) => x + y) //complete here
q5.collect

// COMMAND ----------

// MAGIC %md
// MAGIC ## Exercice 2 (60 mn) 

// COMMAND ----------

// MAGIC %md
// MAGIC Pour cet exercice, on utilise le jeu de données Books qui renseigne sur des livres (books.csv), des utilsateurs (users.csv) et des notes réalisées par les utilsateurs (ratings.csv).
// MAGIC 
// MAGIC Les schémas des tables sont 
// MAGIC 
// MAGIC * `Users (userid: Number, country: Text, age: Number)`
// MAGIC * `Books (bookid: Number, titlewords: Number, authorwords: Number, year: Number, publisher: Number)`
// MAGIC * `Ratings (userid: Number, bookid: Number, rating: Number)`

// COMMAND ----------

val books_data = sc.textFile(path + "books.csv")
val users_data = sc.textFile(path + "users.csv")
val ratings_data = sc.textFile(path + "ratings.csv")




// COMMAND ----------

println("==contenu de books.csv")
books_data.take(10)


// COMMAND ----------

println("==contenu de users.csv")
users_data.take(10)


// COMMAND ----------

println("==contenu de ratings.csv")
ratings_data.take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Préparation

// COMMAND ----------

// MAGIC %md
// MAGIC Afin de formater les données, créer une classe pour chaque table puis charger les données de chaque fichier dans une RDD content des objets de la classe.  

// COMMAND ----------

case class User(userid: Int, country: String, age: Int) {
    override def toString: String = "Users(" + userid + "," + country + "," + age + ")"
}

case class Book(bookid: Int, titlewords: Int, authorwords: Int, year: Int, publisher: Int) {
    override def toString: String = "Books(" + bookid + "," + titlewords + "," +  authorwords + "," + 
    year + "," + publisher + ")"
} 
case class Rating(userid: Int, bookid: Int, rating: Int) {
    override def toString: String = "Rating(" + userid + "," + bookid + "," + rating + ")"
}


// COMMAND ----------

// MAGIC %md
// MAGIC Pour chaque table, créer une RDD contenant des objets de la classe lui correspondant. Compléter les instructions ci-dessous.

// COMMAND ----------

val users = users_data.filter(!_.contains("userid")).map(x=>x.split(",")).map{case Array(u,c,a)=>User(u.toInt,c,a.toInt)}

// COMMAND ----------

users.foreach(println)

// COMMAND ----------

users.take(10)

// COMMAND ----------

val books = books_data.filter(!_.contains("bookid")).map(_.split(",")).map{case Array(b,t,a,y,p) => Book(b.toInt, t.toInt, a.toInt, y.toInt, p.toInt)}

// COMMAND ----------

books.take(10)

// COMMAND ----------

val ratings = ratings_data.filter(!_.contains("userid")).map(_.split(",")).map{case Array(u,b,r) => Rating(u.toInt, b.toInt, r.toInt)}

// COMMAND ----------

ratings.take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC Exprimer les requêtes ci-dessous en opérateurs RDD

// COMMAND ----------

// MAGIC %md
// MAGIC ### Requêtes sur une seule table

// COMMAND ----------

// MAGIC %md
// MAGIC #### Identifiants d'utilisateurs du pays 'france'

// COMMAND ----------

val s0 = users.filter(_.country == "france").map(_.userid)//complete here

// COMMAND ----------

s0.take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Identifiants des livres dont la date est 2000

// COMMAND ----------

val s1 = books.filter(_.year == 2000).map(_.bookid)//complete here

// COMMAND ----------

s1.take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Identifiants des livres notés plus que 3

// COMMAND ----------

val s2 = ratings.filter(_.rating > 3).map(_.bookid)//complete here

// COMMAND ----------

s2.take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Requêtes d'agrégation

// COMMAND ----------

// MAGIC %md
// MAGIC #### Nombres d'utilisateurs par pays, triés par ordre décroissant de ce nombre

// COMMAND ----------

val q1 = users.map(x => (x.country, 1)).reduceByKey((x, y) => x+y).sortBy(_._2, ascending = false) //complete here

// COMMAND ----------

q1.take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Pays qui a le plus grand nombre d'utilisateurs. Il n y a pas d'ex aequo

// COMMAND ----------

val q2 = q1.first

// COMMAND ----------

// MAGIC %md
// MAGIC #### Année avec le plus grand nombre de livres édités. Il n y a pas d'ex aequo

// COMMAND ----------

val q3 = books.map(x => (x.year, 1)).reduceByKey((x, y) => x+y).sortBy(_._2, ascending = false) //complete here
q3.first

// COMMAND ----------

// MAGIC %md
// MAGIC ### Requêtes avec jointure

// COMMAND ----------

// MAGIC %md
// MAGIC #### Les éditeurs de livres ayant été notés par des utilisateurs habitant en France

// COMMAND ----------

val q4 = books.map(x => (x.bookid, x.publisher)).join(ratings.map(x => (x.bookid, x.userid))).map(x => (x._2._2, x._2._1)).join(users.map(x => (x.userid, x.country))).filter(x => x._2._2 == "france").map(x => x._2._1).distinct//complete here
//q4.take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Les éditeurs de livres n'ayant pas été notés par des utilisateurs habitant en France

// COMMAND ----------

val q5 = books.map(_.publisher).subtract(q4).distinct//complete here
q5.take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Pour chaque livre, la moyenne d'age des utilisateurs qui l'ont noté

// COMMAND ----------

val userid_age = users.map(x => (x.userid, x.age))
val userid_bookid = ratings.map(x => (x.userid, x.bookid))

val q6 = userid_age.join(userid_bookid).map(x => (x._2._2, (x._2._1, 1))).reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2)).map(x => (x._1, x._2._1 / x._2._2))//complete here
q6.take(10)

// COMMAND ----------

// MAGIC %md
// MAGIC ### verifications de l'attribut age de users

// COMMAND ----------

val ages = users.map(u=>u.age).distinct
ages.collect

// COMMAND ----------

val ages_bis =  users_data.map(_.split(",")).map(x=>x(2)).distinct
ages_bis.collect

// COMMAND ----------


