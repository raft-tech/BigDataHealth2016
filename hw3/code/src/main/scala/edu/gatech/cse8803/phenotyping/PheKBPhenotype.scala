/**
  * @author Hang Su <hangsu@gatech.edu>,
  * @author Sungtae An <stan84@gatech.edu>,
  */

package edu.gatech.cse8803.phenotyping

import edu.gatech.cse8803.model.{Diagnostic, LabResult, Medication}
import org.apache.spark.rdd.RDD

object T2dmPhenotype {

  // criteria codes given
  val T1DM_DX = Set("250.01", "250.03", "250.11", "250.13", "250.21", "250.23", "250.31", "250.33", "250.41", "250.43",
      "250.51", "250.53", "250.61", "250.63", "250.71", "250.73", "250.81", "250.83", "250.91", "250.93")

  val T2DM_DX = Set("250.3", "250.32", "250.2", "250.22", "250.9", "250.92", "250.8", "250.82", "250.7", "250.72", "250.6",
      "250.62", "250.5", "250.52", "250.4", "250.42", "250.00", "250.02")

  val T1DM_MED = Set("lantus", "insulin glargine", "insulin aspart", "insulin detemir", "insulin lente", "insulin nph", "insulin reg", "insulin,ultralente")

  val T2DM_MED = Set("chlorpropamide", "diabinese", "diabanase", "diabinase", "glipizide", "glucotrol", "glucotrol xl",
      "glucatrol ", "glyburide", "micronase", "glynase", "diabetamide", "diabeta", "glimepiride", "amaryl",
      "repaglinide", "prandin", "nateglinide", "metformin", "rosiglitazone", "pioglitazone", "acarbose",
      "miglitol", "sitagliptin", "exenatide", "tolazamide", "acetohexamide", "troglitazone", "tolbutamide",
      "avandia", "actos", "actos", "glipizide")

  val DM_RELATED_DX = Set("790.21","790.22","790.2","790.29","648.81","648.82","648.83","648.84","648","648","648.01","648.02","648.03","648.04","791.5","277.7","V77.1","256.4") ++ T1DM_DX ++ T2DM_DX

  /**
    * Transform given data set to a RDD of patients and corresponding phenotype
    * @param medication medication RDD
    * @param labResult lab result RDD
    * @param diagnostic diagnostic code RDD
    * @return tuple in the format of (patient-ID, label). label = 1 if the patient is case, label = 2 if control, 3 otherwise
    */
  def transform(medication: RDD[Medication], labResult: RDD[LabResult], diagnostic: RDD[Diagnostic]): RDD[(String, Int)] = {
    /**
      * Remove the place holder and implement your code here.
      * Hard code the medication, lab, icd code etc. for phenotypes like example code below.
      * When testing your code, we expect your function to have no side effect,
      * i.e. do NOT read from file or write file
      *
      * You don't need to follow the example placeholder code below exactly, but do have the same return type.
      *
      * Hint: Consider case sensitivity when doing string comparisons.
      */

    val sc = medication.sparkContext

    /** Hard code the criteria */
    val type1_dm_dx = T1DM_DX
    val type1_dm_med = T1DM_MED
    val type2_dm_dx = T2DM_DX
    val type2_dm_med = T2DM_MED
    val both_med = type1_dm_med ++ type2_dm_med

    /** Find CASE Patients */
    //patients with type 1 diagnosis
    val type1_diag_RDD = diagnostic.filter{diag => type1_dm_dx.contains(diag.code)}
//    println("here")
//    println("type 1 count" + type1_diag_RDD.count())
    // all unique patients
    val all_patients_set = diagnostic.map{diag => diag.patientID}.distinct().collect().toSet

    // patients with no type 1 diagnosis
    val type1_diag_no_patientids_set = all_patients_set diff type1_diag_RDD.map{diag => diag.patientID}.distinct().collect().toSet

    //step 1 of the diagnosis for case patients
    val type1_diag_no_RDD = diagnostic.filter{diag => type1_diag_no_patientids_set.contains(diag.patientID)}

//    println ("No Type1 DM diagnosis (Step1): " + type1_diag_no_RDD.count())

    //patients with type 2 diagnosis
    val type2_diag_step2_set = diagnostic.filter{diag => type2_dm_dx.contains(diag.code)}.map{med => med.patientID}.distinct().collect().toSet

//    type2_diag_step2_set.take(10).foreach(println)
    val two_filters_case_RDD = type1_diag_no_RDD.filter{diag => type2_diag_step2_set.contains(diag.patientID)}
//    println ("Type2 DM Diagnosis (Step2): " + two_filters_case_RDD.count())


    // yes or no for step 3
    val type1_med_yes_set = medication.filter{med => type1_dm_med.contains(med.medicine)}.map{med => med.patientID}.distinct().collect().toSet
    val type1_med_no_set = all_patients_set diff type1_med_yes_set
    val type1_med_yes_RDD = two_filters_case_RDD.filter{med => type1_med_yes_set.contains(med.patientID)}
    val type1_med_no_RDD = two_filters_case_RDD.filter{med => type1_med_no_set.contains(med.patientID)} // CASE

//    println ("Order Type1 DM Medication (Step3 - Yes): " + type1_med_yes_RDD.count())
//    println ("Order Type1 DM Medication (Step3 - No): " + type1_med_no_RDD.count())

    // yes or no for step 4
    val type2_med_yes_set = medication.filter{med => type2_dm_med.contains(med.medicine)}.map{med => med.patientID}.distinct().collect().toSet
    val type2_med_no_set = all_patients_set diff type2_med_yes_set
    val type2_med_yes_RDD = type1_med_yes_RDD.filter{med => type2_med_yes_set.contains(med.patientID)}
    val type2_med_no_RDD = type1_med_yes_RDD.filter{med => type2_med_no_set.contains(med.patientID)} // CASE

//    println ("Order Type2 DM Medication (Step4 - Yes): " + type2_med_yes_RDD.count())
//    println ("Order Type2 DM Medication (Step4 - No): " + type2_med_no_RDD.count())


    val patients_with_type1 = medication.filter{diag => type1_dm_med.contains(diag.medicine)}
    val patients_with_type2 = medication.filter{diag => type2_dm_med.contains(diag.medicine)}

    patients_with_type1.cache()
    patients_with_type2.cache()
    def date_test_list(patient_id : String) : Boolean = {

      val a1 = patients_with_type1.filter(elt => elt.patientID == patient_id)
      val min_date1 = a1.map { elt => elt.date }.distinct().collect().min

      val a2 = patients_with_type2.filter(elt => elt.patientID == patient_id)
      val min_date2 = a2.map { elt => elt.date }.distinct().collect().min

      val date_test = min_date1.getTime - min_date2.getTime > 0

      date_test
    }

    val patients_both_meds_list = type2_med_yes_RDD.map{diag => diag.patientID}.distinct().collect().toSet.toList
    val type2_preceeds_type1_list = patients_both_meds_list.filter{diag => date_test_list(diag)}
    val type2_preceeds_type1_RDD = medication.filter{diag => type2_preceeds_type1_list.contains(diag.patientID)}

//    println ("Type 2 preceeds Type 1 (Step5): " + type2_preceeds_type1_RDD.count())


    //combine all
    val first_no = type1_med_no_RDD.map{diag => diag.patientID}.collect().toSet.toList
    val second_no = type2_med_no_RDD.map{diag => diag.patientID}.collect().toSet.toList
    val third_yes = type2_preceeds_type1_list
    val case_list = first_no ++ second_no ++ third_yes

    println ("Case list: " + case_list.size)
    val casePatients = sc.parallelize(case_list.map{diag => (diag,1)})

    /** Find CONTROL Patients */
    val any_glucose_measure_RDD = labResult.filter{lab => lab.testName.contains("glucose")}

    val abnormal_lab_values_RDD = any_glucose_measure_RDD.filter{lab => test_abnormal(lab)}

    val related_diag_yes_set = diagnostic.filter{diag => DM_RELATED_DX.contains(diag.code)}.map{diag => diag.patientID}.distinct().collect().toSet
    val related_diag_no_set = all_patients_set diff related_diag_yes_set

    val control_RDD = abnormal_lab_values_RDD.filter{diag => related_diag_no_set.contains(diag.patientID)}
    val control_list = control_RDD.map{diag => diag.patientID}.distinct().collect().toList

    println ("control_list: " + control_list.size)

    val controlPatients = sc.parallelize(control_list.map(diag => (diag, 2)))

    /** Find OTHER Patients */
    val other_list = ((all_patients_set diff case_list.toSet) diff control_list.toSet).toList

    println ("others list: " + other_list.size)

    val others = sc.parallelize(other_list.map{diag => (diag, 3)})

    /** Once you find patients for each group, make them as a single RDD[(String, Int)] */
    val phenotypeLabel = sc.union(casePatients, controlPatients, others)

    /** Return */
    phenotypeLabel
  }

  def test_abnormal(labResult : LabResult) : Boolean = labResult match {
    case LabResult (_,_,testName, value) if (testName == "hba1c" || testName == "hemoglobin a1c") && value >= 6 => false
    case LabResult (_,_,testName, value) if (testName == "fasting blood glucose" || testName == "fasting blood" || testName == "glucose" || testName == "glucose, serum") && value >= 110 => false
    case _ => true
  }
}
