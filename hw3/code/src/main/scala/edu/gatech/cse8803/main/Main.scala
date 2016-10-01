

package edu.gatech.cse8803.main

import java.text.SimpleDateFormat

import edu.gatech.cse8803.clustering.{NMF, Metrics}
import edu.gatech.cse8803.features.FeatureConstruction
import edu.gatech.cse8803.ioutils.CSVUtils
import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import edu.gatech.cse8803.phenotyping.T2dmPhenotype
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.{GaussianMixture, KMeans}
import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, Vectors, Vector}
import org.apache.spark.mllib.feature.StandardScaler
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.Source

import java.util.Date


object Main {
  val sc = createContext
  def main(args: Array[String]) {
    import org.apache.log4j.Logger
    import org.apache.log4j.Level

    Logger.getLogger("org").setLevel(Level.WARN)
    Logger.getLogger("akka").setLevel(Level.WARN)

    // val sc = createContext
    val sqlContext = new SQLContext(sc)

    /** initialize loading of data */
    val (medication, labResult, diagnostic) = loadRddRawData(sqlContext)
    val (candidateMedication, candidateLab, candidateDiagnostic) = loadLocalRawData

    /** conduct phenotyping */
    val phenotypeLabel = T2dmPhenotype.transform(medication, labResult, diagnostic)

    /** feature construction with all features */
    val featureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult),
      FeatureConstruction.constructMedicationFeatureTuple(medication)
    )

    val rawFeatures = FeatureConstruction.construct(sc, featureTuples)
    println ("rawFeatures: " + rawFeatures.count)

    val (kMeansPurity, gaussianMixturePurity, nmfPurity) = testClustering(phenotypeLabel, rawFeatures)
    println(f"[All feature] purity of kMeans is: $kMeansPurity%.5f")
    println(f"[All feature] purity of GMM is: $gaussianMixturePurity%.5f")
    println(f"[All feature] purity of NMF is: $nmfPurity%.5f")

        /** feature construction with filtered features */
    val filteredFeatureTuples = sc.union(
      FeatureConstruction.constructDiagnosticFeatureTuple(diagnostic, candidateDiagnostic),
      FeatureConstruction.constructLabFeatureTuple(labResult, candidateLab),
      FeatureConstruction.constructMedicationFeatureTuple(medication, candidateMedication)
    )

    val filteredRawFeatures = FeatureConstruction.construct(sc, filteredFeatureTuples)
    //
    val (kMeansPurity2, gaussianMixturePurity2, nmfPurity2) = testClustering(phenotypeLabel, filteredRawFeatures)
    println(f"[Filtered feature] purity of kMeans is: $kMeansPurity2%.5f")
    println(f"[Filtered feature] purity of GMM is: $gaussianMixturePurity2%.5f")
    println(f"[Filtered feature] purity of NMF is: $nmfPurity2%.5f")
    sc.stop
  }

  def testClustering(phenotypeLabel: RDD[(String, Int)], rawFeatures:RDD[(String, Vector)]): (Double, Double, Double) = {
    import org.apache.spark.mllib.linalg.Matrix
    import org.apache.spark.mllib.linalg.distributed.RowMatrix

    /** scale features */
    val scaler = new StandardScaler(withMean = true, withStd = true).fit(rawFeatures.map(_._2))
    val features = rawFeatures.map({ case (patientID, featureVector) => (patientID, scaler.transform(Vectors.dense(featureVector.toArray)))})
    val rawFeatureVectors = features.map(_._2).cache()


    /** reduce dimension */
    val mat: RowMatrix = new RowMatrix(rawFeatureVectors)
    val pc: Matrix = mat.computePrincipalComponents(10) // Principal components are stored in a local dense matrix.
    val featureVectors = mat.multiply(pc).rows


    val densePc = Matrices.dense(pc.numRows, pc.numCols, pc.toArray).asInstanceOf[DenseMatrix]
    /** transform a feature into its reduced dimension representation */
    def transform(feature: Vector): Vector = {
      Vectors.dense(Matrices.dense(1, feature.size, feature.toArray).multiply(densePc).toArray)
    }

    val num_clusters=3
    val num_iters=20
    val feature_ids=features.map(_._1)

    featureVectors.cache()

    //kmeans
    val k_mean=KMeans.train(featureVectors, num_clusters, num_iters,1,"k-means||",0L).predict(featureVectors)
    val k_mean_result=feature_ids.zip(k_mean)
    val compare_kmean_with_phenotype=k_mean_result.join(phenotypeLabel).map(_._2)
    val k_purity = Metrics.purity(compare_kmean_with_phenotype)
//    val g = Metrics.get_confusion_matrix(compare_kmean_with_phenotype)

    //gmm
    val gmm_clusters= new GaussianMixture().setK(num_clusters).setMaxIterations(num_iters).setSeed(0L).run(featureVectors).predict(featureVectors)
    val gmm_result=feature_ids.zip(gmm_clusters)
    val compare_gmm_with_phenotype=gmm_result.join(phenotypeLabel).map(_._2)
    val gmm_purity = Metrics.purity(compare_gmm_with_phenotype)
    //    val g = Metrics.get_confusion_matrix(compare_gmm_with_phenotype)

    /** NMF */
    val (w, _) = NMF.run(new RowMatrix(rawFeatureVectors), 3, 200)
    // for each row (patient) in W matrix, the index with the max value should be assigned as its cluster type
    val assignments = w.rows.map(_.toArray.zipWithIndex.maxBy(_._1)._2)

    val labels = features.join(phenotypeLabel).map({ case (patientID, (feature, realClass)) => realClass})

    // zip assignment and label into a tuple for computing purity
    val nmfClusterAssignmentAndLabel = assignments.zipWithIndex().map(_.swap).join(labels.zipWithIndex().map(_.swap)).map(_._2)

    // println ("NMF CLUSTERING")
//    val g2 = Metrics.get_confusion_matrix(nmfClusterAssignmentAndLabel)
    val nmfPurity = Metrics.purity(nmfClusterAssignmentAndLabel)

    (k_purity, gmm_purity, nmfPurity)
  }

  /**
    * load the sets of string for filtering of medication
    * lab result and diagnostics
    *
    * @return
    */
  def loadLocalRawData: (Set[String], Set[String], Set[String]) = {
    val candidateMedication = Source.fromFile("data/med_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateLab = Source.fromFile("data/lab_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    val candidateDiagnostic = Source.fromFile("data/icd9_filter.txt").getLines().map(_.toLowerCase).toSet[String]
    (candidateMedication, candidateLab, candidateDiagnostic)
  }

  def loadRddRawData(sqlContext: SQLContext): (RDD[Medication], RDD[LabResult], RDD[Diagnostic]) = {
    /** You may need to use this date format. */
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX")


    // Load each of the respective csv files into SQL tables
    val medicationDF = CSVUtils.loadCSVAsTable(sqlContext , "data/medication_orders_INPUT.csv","medicationDF")

    val labResultDF = CSVUtils.loadCSVAsTable(sqlContext , "data/lab_results_INPUT.csv","labResultDF")
    // For labResultDF, need to get rid of NA values
    val labResult_DF_filtered = sqlContext.sql("select * from labResultDF where Numeric_Result != ''")

    val diagnosticDF = CSVUtils.loadCSVAsTable(sqlContext , "data/encounter_dx_INPUT.csv","diagnosticDF")
    val encounterDF = CSVUtils.loadCSVAsTable(sqlContext , "data/encounter_INPUT.csv","encounterDF")

    // Use Spark SQL to join the encounter and encounter_dx data sets
    val encounterFullDF = sqlContext.sql("select A.Member_ID , A.Encounter_DateTime, B.code from encounterDF A left join diagnosticDF B on A.Encounter_ID = B.Encounter_ID")



    val medication = medicationDF.map{line => new Medication(line(1).toString.toLowerCase, dateFormat.parse(line(11).toString), line(3).toString.toLowerCase)}
    val labResult = labResult_DF_filtered.map { line => new LabResult(line(1).toString.toLowerCase, dateFormat.parse(line(8).toString), line(11).toString.toLowerCase, line(14).toString.replace(",","").toDouble)}
    val diagnostic = encounterFullDF.map{ line => new Diagnostic(line(0).toString.toLowerCase , dateFormat.parse(line(1).toString) , line(2).toString.toLowerCase)}

    (medication, labResult, diagnostic)
  }

  def createContext(appName: String, masterUrl: String): SparkContext = {
    val conf = new SparkConf().setAppName(appName).setMaster(masterUrl)
    new SparkContext(conf)
  }

  def createContext(appName: String): SparkContext = createContext(appName, "local")

  def createContext: SparkContext = createContext("CSE 8803 Homework Two Application", "local")
}
