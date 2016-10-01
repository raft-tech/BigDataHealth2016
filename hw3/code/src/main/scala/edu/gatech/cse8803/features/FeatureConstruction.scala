/**
 * @author Hang Su
 */
package edu.gatech.cse8803.features

import edu.gatech.cse8803.model.{LabResult, Medication, Diagnostic}
import org.apache.spark.SparkContext
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._


object FeatureConstruction {

  /**
   * ((patient-id, feature-name), feature-value)
   */
  type FeatureTuple = ((String, String), Double)

  /**
   * Aggregate feature tuples from diagnostic with COUNT aggregation,
   * @param diagnostic RDD of diagnostic
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */

//    scala> val inputrddtest = sc.parallelize{Seq(("somepatient","somecode"),("somepatient1","somecode1"),("somepatient", "somecode"))}
//    scala> val count = inputrddtest.countByValue
//    count: scala.collection.Map[(String, String),Long] = Map((somepatient1,somecode1) -> 1, (somepatient,somecode) -> 2)
//    scala> (count.keys.toList zip count.values.map{m => m.toDouble}.toList).toMap.toList
//    res15: List[((String, String), Double)] = List(((somepatient1,somecode1),1.0), ((somepatient,somecode),2.0))

    val diags = diagnostic.map{diag => (diag.patientID, diag.code)}
    val diagsCounts = diags.countByValue
    val diagsList = (diagsCounts.keys.toList zip diagsCounts.values.map{v => v.toDouble}.toList).toMap.toList
    println ("diagslist: " + diagsList.size)
    diagnostic.sparkContext.parallelize(diagsList)

  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation,
   * @param medication RDD of medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val meds = medication.map{med => (med.patientID, med.medicine)}
    val medsCounts = meds.countByValue
    val medsList = (medsCounts.keys.toList zip medsCounts.values.map{v => v.toDouble}.toList).toMap.toList
    println ("medsList: " + medsList.size)
    medication.sparkContext.parallelize(medsList)
  }

  def sumByKeys[A](tuples: List[(A, Double)]): List[(A, Double)] =
    tuples groupBy (_._1) map { case (k, v) => (k, v.map(_._2).sum) } toList

  /**
   * Aggregate feature tuples from lab result, using AVERAGE aggregation
   * @param labResult RDD of lab result
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
//    case class LabResult(patientID: String, testName: String, value: Double)
//    val labRDD = sc.parallelize(List(LabResult("patient1","glucose",10.0), LabResult("patient1", "blood", 11.0), LabResult("patient1","blood", 21.0), LabResult("patient2","blood", 5.0), LabResult("patient2", "glucose", 14.0), LabResult("patient2","blood", 10.0)))
//    val labs = labRDD.map{lab => ((lab.patientID, lab.testName),lab.value)}
//    val labsList = labs.collect().toList
//    val labSums = sumByKeys(labsList)
//    val lab_counts = labRDD.map{lab => (lab.patientID, lab.testName)}.countByValue
//    labSums.keys.toList.map{key => (key, labSums(key) / lab_counts(key))}
    val labs = labResult.map{lab => ( (lab.patientID, lab.testName), lab.value)}
    val lab_counts = labResult.map{elt => (elt.patientID , elt.testName)}.countByValue
    val labs_list = labs.collect().toList
    val lab_sums = sumByKeys(labs_list).toMap
    val lab_avgs = lab_sums.keys.toList.map{elt => (elt , lab_sums(elt) / lab_counts(elt))}
    println ("lab_avgs: " + lab_avgs.size)
    labResult.sparkContext.parallelize(lab_avgs)
  }

  /**
   * Aggregate feature tuple from diagnostics with COUNT aggregation, but use code that is
   * available in the given set only and drop all others.
   * @param diagnostic RDD of diagnostics
   * @param candiateCode set of candidate code, filter diagnostics based on this set
   * @return RDD of feature tuples
   */
  def constructDiagnosticFeatureTuple(diagnostic: RDD[Diagnostic], candiateCode: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */

    val filtered_diag = diagnostic.filter{diag => candiateCode.contains(diag.code)}
    val diags = filtered_diag.map{diag => (diag.patientID, diag.code)}
    val diagsCounts = diags.countByValue
    val diagsList = (diagsCounts.keys.toList zip diagsCounts.values.map{v => v.toDouble}.toList).toMap.toList
    println ("diagslist (candidate): " + diagsList.size)
    diagnostic.sparkContext.parallelize(diagsList)
//    diagnostic.sparkContext.parallelize(List((("patient", "diagnostics"), 1.0)))
  }

  /**
   * Aggregate feature tuples from medication with COUNT aggregation, use medications from
   * given set only and drop all others.
   * @param medication RDD of diagnostics
   * @param candidateMedication set of candidate medication
   * @return RDD of feature tuples
   */
  def constructMedicationFeatureTuple(medication: RDD[Medication], candidateMedication: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val filtered_meds = medication.filter{med => candidateMedication.contains(med.medicine)}
    val meds = filtered_meds.map{med => (med.patientID, med.medicine)}
    val medsCounts = meds.countByValue
    val medsList = (medsCounts.keys.toList zip medsCounts.values.map{v => v.toDouble}.toList).toMap.toList
    println ("medsList (candidate): " + medsList.size)
    medication.sparkContext.parallelize(medsList)
//    medication.sparkContext.parallelize(List((("patient", "med"), 1.0)))
  }


  /**
   * Aggregate feature tuples from lab result with AVERAGE aggregation, use lab from
   * given set of lab test names only and drop all others.
   * @param labResult RDD of lab result
   * @param candidateLab set of candidate lab test name
   * @return RDD of feature tuples
   */
  def constructLabFeatureTuple(labResult: RDD[LabResult], candidateLab: Set[String]): RDD[FeatureTuple] = {
    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
//    labResult.sparkContext.parallelize(List((("patient", "lab"), 1.0)))
    val filtered_labs = labResult.filter{lab => candidateLab.contains(lab.testName)}
    val labs = filtered_labs.map{lab => ( (lab.patientID, lab.testName), lab.value)}
    val lab_counts = filtered_labs.map{lab => (lab.patientID , lab.testName)}.countByValue
    val labs_list = labs.collect().toList
    val lab_sums = sumByKeys(labs_list).toMap
    val lab_avgs = lab_sums.keys.toList.map{elt => (elt , lab_sums(elt) / lab_counts(elt))}

    println ("lab_avgs (candidate): " + lab_avgs.size)
    labResult.sparkContext.parallelize(lab_avgs)
  }


  /**
   * Given a feature tuples RDD, construct features in vector
   * format for each patient. feature name should be mapped
   * to some index and convert to sparse feature format.
   * @param sc SparkContext to run
   * @param feature RDD of input feature tuples
   * @return
   */
  def construct(sc: SparkContext, feature: RDD[FeatureTuple]): RDD[(String, Vector)] = {

    /** save for later usage */
    feature.cache()

    /** create a feature name to id map*/

    val feature_list = feature.collect().toList
    //List(((000527376-01,v70.0),4.0), ((666932816-01,v70.0),5.0), ((000041840-01,v70.0),4.0), ((012820000-01,584.9),17.0))
    val feature_map = feature_list.map{tupple => tupple._1._2}.distinct.zipWithIndex.toMap
    /** transform input feature */

    //assign index values to features
    val f_with_index = feature.map {f => (f._1._1, (feature_map(f._1._2), f._2))}.collect.toList
    val f_group = f_with_index groupBy (_._1)

    val ps = f_group.keys.toList
    val inlist = ps.map{p => (p, f_group(p).map{f => f._2})}
    val vector_list = inlist.map{v => (v._1, Vectors.sparse(feature_map.size, v._2))}

    println ("printon: " + vector_list.take(5))
    /**
     * Functions maybe helpful:
     *    collect
     *    groupByKey
     */

    /**
     * TODO implement your own code here and remove existing
     * placeholder code
     */
    val result = sc.parallelize(vector_list)
//    val result = sc.parallelize(Seq(("Patient-NO-1", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
//                                    ("Patient-NO-2", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
//                                    ("Patient-NO-3", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
//                                    ("Patient-NO-4", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
//                                    ("Patient-NO-5", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
//                                    ("Patient-NO-6", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
//                                    ("Patient-NO-7", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
//                                    ("Patient-NO-8", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
//                                    ("Patient-NO-9", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)),
//                                    ("Patient-NO-10", Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0))))
    result

  }
}


