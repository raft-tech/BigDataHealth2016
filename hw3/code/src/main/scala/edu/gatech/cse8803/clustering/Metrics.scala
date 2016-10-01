/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.clustering

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import breeze.linalg.max

object Metrics {
  /**
   * Given input RDD with tuples of assigned cluster id by clustering,
   * and corresponding real class. Calculate the purity of clustering.
   * Purity is defined as
   *             \fract{1}{N}\sum_K max_j |w_k \cap c_j|
   * where N is the number of samples, K is number of clusters and j
   * is index of class. w_k denotes the set of samples in k-th cluster
   * and c_j denotes set of samples of class j.
   * @param clusterAssignmentAndLabel RDD in the tuple format
   *                                  (assigned_cluster_id, class)
   * @return purity
   */
  def purity(clusterAssignmentAndLabel: RDD[(Int, Int)]): Double = {
    /**
     * TODO: Remove the placeholder and implement your code here
     */
    clusterAssignmentAndLabel.map(f =>(f,1))
      .keyBy(_._1)
      .reduceByKey((k,v) =>(k._1,k._2+v._2))
      .map(f => (f._2._1._1,f._2._2))
      .keyBy(_._1)
      .reduceByKey((k,v) =>(1,max(k._2,v._2)))
      .map(f => f._2._2)
      .reduce((x,y) => x+y) / clusterAssignmentAndLabel.count().toDouble
  }

  def get_confusion_matrix(clusterAssignmentAndLabel: RDD[(Int, Int)]): Double = {

    //val N = clusterAssignmentAndLabel.count.toDouble
    val K =  clusterAssignmentAndLabel.map{elt => elt._1}.distinct.collect.toList.sorted

    println ("K: " + K) //0,1,2

    val all_classes = clusterAssignmentAndLabel.map{elt => elt._2}.distinct.collect().toList.sorted

    println ("classes: " + all_classes) //1,2,3

    val cluster_labels = clusterAssignmentAndLabel.collect.toList

//    println ("labels: " + cluster_labels)

    //val class_to_cluster_map =  (all_classes , K).zipped.toMap

    val by_class = all_classes.map{elt => cluster_labels.filter{x => x._2 == elt}}

    println ("by_class: " + by_class.size)

    val matrix1 = by_class(0)
//    println ("0: " + by_class(0))
    val matrix2 = by_class(1)
//    println ("1: " + by_class(1))
    val matrix3 = by_class(2)
//    println ("2: " + by_class(2))

    val confusion_list =  List( K.map{k => matrix1.count(_._1==k)},
      K.map{k => matrix2.count(_._1==k)},
      K.map{k => matrix3.count(_._1==k)} )

    println("list: " + confusion_list.size)

    val col1 = List(0,1,2).map{elt => confusion_list(elt)(0)}
    val col2 = List(0,1,2).map{elt => confusion_list(elt)(1)}
    val col3 = List(0,1,2).map{elt => confusion_list(elt)(2)}



    println("Cluster 1")
    println(col1.map{elt => elt / col1.sum.toDouble})
    println("\n\nCluster 2")
    println(col2.map{elt => elt / col2.sum.toDouble})
    println("\n\nCluster 3")
    println(col3.map{elt => elt / col3.sum.toDouble})
    println("\n\n")


    val result = 0.0

    result

  }

}
