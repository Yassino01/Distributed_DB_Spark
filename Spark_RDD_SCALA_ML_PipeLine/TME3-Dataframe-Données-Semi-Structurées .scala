// Databricks notebook source
// MAGIC %md
// MAGIC * Author: Mohamed-Amine Baazizi 
// MAGIC * Affiliation: LIP6 - Faculté des Sciences - Sorbonne Université
// MAGIC * Email: mohamed-amine.baazizi@lip6.fr

// COMMAND ----------

// MAGIC %md
// MAGIC # Interrogation de données semi-structurées en Spark

// COMMAND ----------

// MAGIC %md
// MAGIC Ce TME illustre une chaine de traitement allant de la préparation de données à la formulation de requêtes d'analyse. 
// MAGIC Les données utilisées sont au format JSON et présentent des variations structurelles dont il faudra tenir compte pendant la préparation des données.
// MAGIC 
// MAGIC Les traitements s'expriement dans l'API Dataset dont la documentation est accessible depuis ces deux liens :
// MAGIC * https://spark.apache.org/docs/latest/sql-programming-guide.html
// MAGIC * https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html

// COMMAND ----------

// MAGIC %md
// MAGIC Les données utilisées correspondent à des évènements décrivant des posts échangés, partagés ou commentés sur un réseau social VK pendant l'élection présidentielle Russe de 2018. L'API officielle de ces données est décrite sur le lien https://vk.com/dev/streaming_api_docs_2?f=7.%2BЧтение%2Bпотока et les données sont tirées d'un challenge Kaggle https://www.kaggle.com/borisch/russian-election-2018-vkcom-user-activity   
// MAGIC 
// MAGIC Nous utilisons ces données pour analyser les posts par types, les tags utilisés dans ces posts et la relation implicite entre les auteurs des posts.
// MAGIC 
// MAGIC Nous utilisons un échantillon qui représente 1% des données d'origine 
// MAGIC 
// MAGIC Télécharger l'archive  `VKRU18_001.json.gz` accessible depuis https://nuage.lip6.fr/s/b2Ajc8Wc6FFGjRd 
// MAGIC 
// MAGIC puis copier `VKRU18_001.json`  dans  `/FileStore/tables/BDLE/TME3/`

// COMMAND ----------

// MAGIC %md
// MAGIC ## Questions

// COMMAND ----------

// MAGIC %md
// MAGIC ### Chargement des données 

// COMMAND ----------

val path = "/FileStore/tables/BDLE/TME3/"

// COMMAND ----------

val vk = path + "VKRU18_001.json"
val data = spark.read.format("json").load(vk).dropDuplicates
val total = data.count()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Affichage du schema 

// COMMAND ----------

data.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC #### Fréquence d'apparition des attributs optionnels

// COMMAND ----------

// MAGIC %md
// MAGIC Le schéma extrait est peu informatif quant à l'optionalité des attributs puisque pour l'attribut `nullable` est mis à `true` par défaut.
// MAGIC Les instructions suivantes retournent la fréqunce d'attributs qui sont utiles à l'analyse.

// COMMAND ----------

val attrs = List("event", 
                 "event.event_id", 
                 "event.event_id.post_id", 
                 "event.event_id.post_owner_id", 
                 "event.event_id.comment_id", 
                 "event.event_id.shared_post_id", 
                 "event.author", 
                 "event.attachments", 
                 "event.geo", 
                 "event.tags",
                 "event.creation_time")

attrs.foreach(x=>println(x + " "+data.where(x+" is not null").count))

// COMMAND ----------

// MAGIC %md
// MAGIC #### Quels attributs sont optionnels lesquels sont obligatoires? 
// MAGIC D'apres la documentation officielle https://vk.com/dev/streaming_api_docs_2?f=7.%2BЧтение%2Bпотока il est indiqué que la présence de certains attributs est conditionnée par la valeur que prend un autre attribut.
// MAGIC Par exemple, `event.event_id.comment_id` n'est présent que si `event.event_type='comment'`.
// MAGIC Idem pour `event.event_id.shared_post_id` qui n'est présent que si `event.event_type='share'`.

// COMMAND ----------

// MAGIC %md
// MAGIC #### Vérifier ces hypothèses à l'aide de requêtes

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC vérifier  que `event.event_type='comment'` implique `event.event_id.comment_id`

// COMMAND ----------

/*vérifier  que `event.event_type='comment'` implique
`event.event_id.comment_id` */
data.where("event.event_type!='comment' and event.event_id.comment_id is not null").count() == 0


// COMMAND ----------

// MAGIC %md 
// MAGIC vérifier que `event.event_type='share'` implique `event.event_id.shared_post_id` 

// COMMAND ----------

/*vérifier que `event.event_type='share'` implique 
`event.event_id.shared_post_id` */
data.where("event.event_type!='share' and event.event_id.shared_post_id is not null").count() == 0


// COMMAND ----------

// MAGIC %md
// MAGIC #### Combien y a t il  de post id différents ?

// COMMAND ----------

data.select("event.event_id.post_id").where("event.event_id.post_id is not null").distinct().count()


// COMMAND ----------

// MAGIC %md
// MAGIC #### Retourner le nombre d'objets distincts par type d'évenement 

// COMMAND ----------

val postPerType = data.groupBy("event.event_type").agg(countDistinct(col("event.event_id.post_id")))
postPerType.show()



// COMMAND ----------

// MAGIC %md
// MAGIC #### Applatissement des listes de tags
// MAGIC Dans la valeur `data` chaque objet est associé à un tableau de tags accessible depuis `event.tags`.
// MAGIC L'instruction suivante applatie ce tableau en créant une ligne pour chaque chaine contenue dans tags.

// COMMAND ----------

data.printSchema()

// COMMAND ----------

val dataWithTags = data.select(col("_id"), col("code"), col("event"), explode($"event.tags") as "tag")

dataWithTags.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC #### Retourner le nombre d'objets distincts par tag, triés de façon décroissante

// COMMAND ----------

val objectsPerTag = dataWithTags.groupBy("tag").agg(countDistinct("event.event_id.post_id") as "count").orderBy(col("count").desc)

objectsPerTag.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ####  Retourner, pour chaque tag, le nombre d'auteurs distincts, triés de façon décroissante

// COMMAND ----------

val authCountPerTag = dataWithTags.groupBy("tag").agg(countDistinct("event.author.id") as "nbAuths").orderBy(col("nbAuths").desc)

authCountPerTag.show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### Fact-checking en utilisant Wikipédia
// MAGIC Chaque tag correspond à un candidat de l'élection Russe de 2018 (ex. Putin, Titov, etc).
// MAGIC Nous voulons savoir s'il existe une relation entre le nombre de tag par auteur et le nombre de votes de chaque candidat.
// MAGIC On récupere depuis Wikipedia le nombre de votes de chaque candidat et on le stocke dans la valeur `votes` décrite ci-dessous.

// COMMAND ----------

import spark.implicits._

case class Vote(name: String, party: String, votes: Long)
val votes = Seq(Vote("putin", "Independent", 56430712), 
                Vote("grudinin", "Communist",8659206), 
                Vote("zhirinovsky","Liberal Democratic Party",4154985),
                Vote("sobchak","Civic Initiative",1238031),
                Vote("yavlinsky","Yabloko",769644), 
                Vote("titov","Party of Growth",556801)).toDS()
votes.printSchema
votes.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Créeer une table contenant pour chaque candidat le nombre de votes et le nombre d'auteurs ayant utilisé son nom comme  tag

// COMMAND ----------

val votesCount = votes.join(authCountPerTag).select("name", "votes", "nbAuths").where("tag=name")

votesCount.orderBy($"nbAuths".desc).show

// COMMAND ----------

// MAGIC %md
// MAGIC #### Rajouter à la table de votes le rang obtenu à partir du nombre de votes et celui obtenu à partir du nombre d'auteurs. Que remarquez-vous ?

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

//val windowVotes  = Window.orderBy(col("votes"))
//val windowNbAuths  = Window.orderBy("nbAuths")

votesCount.withColumn("votes_rank", row_number.over(Window.orderBy($"votes".desc))).withColumn("nbAuths_rank", row_number.over(Window.orderBy($"nbAuths".desc))).show()

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._



// COMMAND ----------

// MAGIC %md
// MAGIC ### Création de cubes
// MAGIC Nous voulons créer une cube pour agréger les objets sur trois dimensions : tag, type d'évenement et mois de création (attribut `event.creation_time`)
// MAGIC #### Rajouter un attribut month contenant le mois extrait pour chaque objet 
// MAGIC Astuce. importer le package org.apache.spark.sql.types.DateType et utiliser une fonction Spark SQL prédéfinie.
// MAGIC Vous remarquerez qu'il n y a que trois mois : 1,2 et 3.

// COMMAND ----------

import org.apache.spark.sql.types.DateType
// timestamp(from_unixtime(1563853753,'yyyy-MM-dd HH:mm:ss')
val dataTagMon = dataWithTags.withColumn("month", month(from_unixtime(col("event.creation_time"), "yyyy-MM-dd HH:mm:ss").cast(DateType)))

dataTagMon.select("month").printSchema()

// COMMAND ----------

import org.apache.spark.sql.types.DateType
val dataTagMon = dataWithTags.

dataTagMon.select("month").printSchema()

// COMMAND ----------

dataTagMon.where("month > 3").count

// COMMAND ----------

// MAGIC %md
// MAGIC #### Que fait l'instruction suivante ? 

// COMMAND ----------

val tag_event_month = dataTagMon.groupBy("event.event_type","tag","month").count()
tag_event_month.orderBy($"count".desc).show()

// COMMAND ----------

// MAGIC %md
// MAGIC ### Pivot 
// MAGIC #### En utilisant le cube construit précédemment, construire une table pivot en réduisant le cube précédent aux dimensions mois et type d'évenement.

// COMMAND ----------

val monthEvent = tag_event_month.groupBy($"month").pivot("event_type").agg(sum($"count"))

monthEvent.printSchema
monthEvent.show

// COMMAND ----------

// MAGIC %md
// MAGIC ###  Matrice de co-occurrence des tag
// MAGIC #### Créer une table indiquant pour chaque paire de tag  l'auteur qui les utilise.

// COMMAND ----------

val authTag = dataWithTags.select(col("event.author.id") as "authorID", col("tag"))

val tagCoOcc = authTag.withColumnRenamed("tag", "otherTag").join(authTag, "authorID").where("tag!=otherTag")

tagCoOcc.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### retourner pour chaque paire de tags le nombre d'auteur distincts qui les utilisent, tri décroissant sur ce nomber

// COMMAND ----------

val tagCoOccNB = tagCoOcc.groupBy("tag", "otherTag").agg(countDistinct("authorID") as "count").orderBy($"count".desc)

tagCoOccNB.show()

// COMMAND ----------

// MAGIC %md
// MAGIC #### Construire une matrice de co-occurrence des tags en utilisant la fonction `pivot`

// COMMAND ----------

// même résultat que le prof mais il est faux
val tagMat = tagCoOcc.groupBy("tag").pivot("otherTag").agg(sum("authorID"))

tagMat.show()


// COMMAND ----------

// Véritable façon de compter les occurences
val tagMat = tagCoOcc.groupBy("tag").pivot("otherTag").agg(count("authorID"))

tagMat.show()


// COMMAND ----------

// MAGIC %md
// MAGIC #### Construire une table indiquant pour chaque type d'évenement, le nombre de fois que chaque platforme est utilisée.

// COMMAND ----------

val eventTypePlatform = data.groupBy("event.event_type").pivot("event.author.platform").agg(count("*"))

eventTypePlatform.show()

// COMMAND ----------

// MAGIC %md
// MAGIC On sait d'après https://vk.com/dev/streaming_api_docs_2?f=7.%2BЧтение%2Bпотока que 
// MAGIC platform (integer) – prend l'une des valeurs suivantes :
// MAGIC * 1 – mobile web version;
// MAGIC * 2 – iPhone;
// MAGIC * 3 – iPad;
// MAGIC * 4 – Android;
// MAGIC * 5 – Windows Phone;
// MAGIC * 6 – Windows 8;
// MAGIC * 7 – full web version;
// MAGIC * 8 – other apps
// MAGIC 
// MAGIC Reprendre la question précédente en remplaçant les identifiant de platform par leur nom textuel

// COMMAND ----------

val init = seq(1, 2)
val ren = ("mobile_web_version", "iPhone")

val renamedColumns = seq.map(name => col(name).as(re))
someDataframe.select(renamedColumns : _*)

val eventTypePlatformLabels = eventTypePlatform.withColumnRenamed

// COMMAND ----------

// MAGIC %md 
// MAGIC ##Fin
