// Databricks notebook source
// MAGIC %md
// MAGIC * Author: Mohamed-Amine Baazizi
// MAGIC * Affiliation: LIP6 - Faculté des Sciences - Sorbonne Université
// MAGIC * Email: mohamed-amine.baazizi@lip6.fr
// MAGIC * Master DAC, BDLE, Novembre 2020

// COMMAND ----------

// MAGIC %md
// MAGIC #  Induction à Spark ML

// COMMAND ----------

// MAGIC %md
// MAGIC Ce TME vise à illustrer 
// MAGIC * l'encodage des features pour l'inférence des arbres de décisions
// MAGIC * la sélection de modèle 
// MAGIC 
// MAGIC Il utilise des données synthetiques de taille réduite : `crédit` et `météo`

// COMMAND ----------

// MAGIC %md
// MAGIC ## Partie 1 : Arbre de décision pour la classification (exemple du cours)
// MAGIC Cette partie reprend les exemples du cours qui utilise les données `crédit`. Il suffit d'exécuter et d'observer le résultat

// COMMAND ----------

import spark.implicits._

case class tuple(age: String,income: String,student: String,credit_rating: String,label: String)

val data = Seq(tuple("young","high","no","fair","no"),
               tuple("young","high","no","excellent","no"),
               tuple("middle","high","no","fair","yes"),
               tuple("senior","medium","no","fair","yes"),
               tuple("senior","low","yes","fair","yes"),
               tuple("senior","low","yes","excellent","no"),
               tuple("middle","low","yes","excellent","yes"),
               tuple("young","medium","no","fair","no"),
               tuple("young","low","yes","fair","yes"),
               tuple("senior","medium","yes","fair","yes"),
               tuple("young","medium","yes","excellent","yes"),
               tuple("middle","medium","no","excellent","yes"),
               tuple("middle","high","yes","fair","yes"),
               tuple("senior","medium","no","excellent","no")).toDS()
data.printSchema
data.show()

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Encodage des features

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Indexer les string

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer

//la variable à prédire 
val label = "label"
val labelIndexer = new StringIndexer()
                    .setInputCol(label)
                    .setOutputCol("indexed_" + label)


val featureIndexer = data.columns.filterNot(_.contains(label)).map{field => 
    val stringIndexer = new StringIndexer()
                    .setInputCol(field)
                    .setOutputCol("indexed_" + field)
        stringIndexer}

// COMMAND ----------

featureIndexer

// COMMAND ----------

// MAGIC %md 
// MAGIC #### assembler les features en un vecteur

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler

val features = featureIndexer.map(_.getOutputCol)
//assemble with the rest of the features
val vecAssemb = new VectorAssembler()
        .setInputCols(features)
        .setOutputCol("assembled")


// COMMAND ----------

// MAGIC %md 
// MAGIC #### indexer les vecteurs

// COMMAND ----------

import org.apache.spark.ml.feature.VectorIndexer

val vecIndexer = new VectorIndexer()
                .setInputCol(vecAssemb.getOutputCol)
                .setOutputCol("features")
                .setMaxCategories(4) 


// COMMAND ----------

// MAGIC %md 
// MAGIC #### assembler dans un pipeline

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val pipeline = new Pipeline().setStages(Array(labelIndexer) ++ featureIndexer ++ Array(vecAssemb,vecIndexer))
var trainData = pipeline.fit(data).transform(data)


// COMMAND ----------

trainData.printSchema()
trainData.select("features", "indexed_label").show()

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Inférence du modèle

// COMMAND ----------

import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier

val dt = new DecisionTreeClassifier()
  .setLabelCol("indexed_label")
  .setFeaturesCol("features")

val dtModel = dt.fit(trainData)
println(s"Learned classification tree model:\n ${dtModel.toDebugString}")

// COMMAND ----------

// MAGIC %md 
// MAGIC ## Partie 2 : Arbre de décision pour la regression (travail à faire)
// MAGIC Cette partie est à compléter. Elle utilise les données `météo` pour l'inférence d'un arbre pour la regression permettant de prédire la variable `hours` en fonction de 4 characteristiques (`outlook`, `temp`, `humidity` et `windy`).
// MAGIC 
// MAGIC Vous pouvez vous inspirer de la partie 1 pour l'encodage des features.

// COMMAND ----------

import spark.implicits._

case class tuple(outlook: String,temp: String,humidity: String,windy: String,hours: Double)

val data = Seq(tuple("rainy","hot","high","FALSE",25.0),
tuple("rainy","hot","high","TRUE",30.0),
tuple("overcast","hot","high","FALSE",46.0),
tuple("sunny","mild","high","FALSE",45.0),
tuple("sunny","cool","normal","FALSE",52.0),
tuple("sunny","cool","normal","TRUE",23.0),
tuple("overcast","cool","normal","TRUE",43.0),
tuple("rainy","mild","high","FALSE",35.0),
tuple("rainy","cool","normal","FALSE",38.0),
tuple("sunny","mild","normal","FALSE",46.0),
tuple("rainy","mild","normal","TRUE",48.0),
tuple("overcast","mild","high","TRUE",52.0),
tuple("overcast","hot","normal","FALSE",44.0),
tuple("sunny","mild","high","TRUE",30.0)
).toDS

data.printSchema()
data.show()

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Encodage des features
// MAGIC Reprendre les mêmes étapes d'encodages des features que pour la partie 1. 
// MAGIC Faire en sorte que toutes les features soient categoricielles!

// COMMAND ----------

// MAGIC %md 
// MAGIC ##### Indexation des Strings

// COMMAND ----------

import org.apache.spark.ml.feature.StringIndexer

//la variable à prédire 
val label = "hours"
//val labelIndexer = new StringIndexer()
//                   .setInputCol(label)
//                    .setOutputCol("indexed_" + label)

val f = data.columns.filterNot(_.contains(label))
print(f)
val featureIndexer = data.columns.filterNot(_.contains(label)).map{field => 
    val stringIndexer = new StringIndexer()
                    .setInputCol(field)
                    .setOutputCol("indexed_" + field)
        stringIndexer}

// COMMAND ----------

// MAGIC %md
// MAGIC ##### Assemblage des features en un vecteur

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler

val features = featureIndexer.map(_.getOutputCol)
//assemble with the rest of the features
val vecAssemb = new VectorAssembler()
        .setInputCols(features)
        .setOutputCol("assembled")


// COMMAND ----------

// MAGIC %md 
// MAGIC ##### Indexation des vecteurs

// COMMAND ----------

import org.apache.spark.ml.feature.VectorIndexer

val vecIndexer = new VectorIndexer()
                .setInputCol(vecAssemb.getOutputCol)
                .setOutputCol("features")
                .setMaxCategories(6) 


// COMMAND ----------

// MAGIC %md 
// MAGIC #### Regroupement des traitements dans une pipeline

// COMMAND ----------

import org.apache.spark.ml.Pipeline

val pipeline = new Pipeline().setStages(featureIndexer ++ Array(vecAssemb,vecIndexer))
var trainData = pipeline.fit(data).transform(data)

// COMMAND ----------

trainData.printSchema()
trainData.select("features", "hours").show()

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Inférence du modèle
// MAGIC Consulter la documentation https://spark.apache.org/docs/latest/ml-classification-regression.html#decision-tree-regression 
// MAGIC puis inférer un arbre de regression pour les données météo

// COMMAND ----------

print(label)

// COMMAND ----------

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorIndexer
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.regression.DecisionTreeRegressor

// Load the data stored in LIBSVM format as a DataFrame.
// Split the data into training and test sets (30% held out for testing).

//rename column hours to label
trainData = trainData.withColumnRenamed("hours", "label")


// Train a DecisionTree model.
val dt = new DecisionTreeRegressor()
  .setLabelCol("label")
  .setFeaturesCol("features")



// Train model. This also runs the indexer.
val model = dt.fit(trainData)

// Make predictions.
val predictions = model.transform(trainData)

// Select example rows to display.
predictions.select("prediction", "label", "features").show(5)

// Select (prediction, true label) and compute test error.
val evaluator = new RegressionEvaluator()
  .setLabelCol("label")
  .setPredictionCol("prediction")
  .setMetricName("rmse")
val rmse = evaluator.evaluate(predictions)
println(s"Root Mean Squared Error (RMSE) on test data = $rmse")

val treeModel = model.asInstanceOf[DecisionTreeRegressionModel]
println(s"Learned regression tree model:\n ${treeModel.toDebugString}")

// COMMAND ----------

// MAGIC %md ## Partie 3 : Model tuning
// MAGIC Cette partie illustre la recherche du meilleur modèle sur les données météo.
// MAGIC Le but est de varier certains parametres avec `ParamGridBuilder` et de choisir différentes paires de données training et test avec `CrossValidator`.
// MAGIC Attention les données météo sont  petites et ne montrent pas l'interet de ce travail mais cette partie est utile pour votre mini-projet.
// MAGIC 
// MAGIC Lire la documentation is here https://spark.apache.org/docs/latest/ml-tuning.html 

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Grille de parametres

// COMMAND ----------

import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
 
//considerer que dt est le DecisionTreeRegressionModel de la partie 2 

//create a grid setting maxBins and minInstancesPerNode parameters
val dt_paramGrid = new ParamGridBuilder()
        .addGrid(dt.maxDepth, Array(10, 20, 30)) //essayer un parametre de votre choix 
        .addGrid(dt.maxBins, Array(10,20, 40, 80, 100)) //essayer un autre parametre de votre choix 
        .build()

// COMMAND ----------

//create k folds with k=3 et degré de parallelisme = 2
val cv = new CrossValidator()
            .setEstimator(dt)
            .setEstimatorParamMaps(dt_paramGrid)
            .setEvaluator(new RegressionEvaluator)
            .setNumFolds(3)  
            .setParallelism(2)  

// COMMAND ----------

//train the different models
val cvModel = cv.fit(trainData) //ftdata correspond aux données apres encodage des features  

// COMMAND ----------

val bestModel = cvModel.bestModel.asInstanceOf[DecisionTreeRegressionModel]


// COMMAND ----------

bestModel.extractParamMap() 

// COMMAND ----------

display(bestModel)

// COMMAND ----------

getParams(bestModel)
