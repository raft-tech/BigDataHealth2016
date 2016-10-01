import time
import numpy as n
import pandas as p
import utils

def deceased_patients_on_patient_id(events, mortality):
    return p.merge(events, mortality, on='patient_id')

def patients_not_in_deceased(events, deceased_patients):
    return events[~events.patient_id.isin(deceased_patients.patient_id)]
def read_csv(filepath):
    '''
    Read the events.csv and mortality_events.csv files. Variables returned from this function are passed as input to the metric functions.
    This function needs to be completed.
    '''
    events = p.read_csv(filepath + 'events.csv')
    mortality = p.read_csv(filepath + 'mortality_events.csv')

    return events, mortality

def event_count_metrics(events, mortality):
    '''
    Event count is defined as the number of events recorded for a given patient.
    This function needs to be completed.
    '''
    #deceased patients
    deceased_patients = deceased_patients_on_patient_id(events, mortality)
    aggregate_dead_patients = deceased_patients.groupby(['patient_id']).count()
    avg_dead_event_count = aggregate_dead_patients['event_id'].mean()
    max_dead_event_count = aggregate_dead_patients['event_id'].max()
    min_dead_event_count = aggregate_dead_patients['event_id'].min()

    #living patients
    living_patients = patients_not_in_deceased(events, deceased_patients)
    aggregate_living_patients = living_patients.groupby(['patient_id']).count()
    avg_alive_event_count = aggregate_living_patients['event_id'].mean()
    max_alive_event_count = aggregate_living_patients['event_id'].max()
    min_alive_event_count = aggregate_living_patients['event_id'].min()

    return min_dead_event_count, max_dead_event_count, avg_dead_event_count, min_alive_event_count, max_alive_event_count, avg_alive_event_count

def encounter_count_metrics(events, mortality):
    '''
    Encounter count is defined as the count of unique dates on which a given patient visited the ICU.
    This function needs to be completed.
    '''

    deceased_patients = deceased_patients_on_patient_id(events, mortality)
    aggregate_dead_patients = deceased_patients.groupby(['patient_id']).timestamp_x.nunique()
    avg_dead_encounter_count = aggregate_dead_patients.mean()
    max_dead_encounter_count = aggregate_dead_patients.max()
    min_dead_encounter_count = aggregate_dead_patients.min()

    living_patients = patients_not_in_deceased(events, deceased_patients)
    aggregate_living_patients = living_patients.groupby(['patient_id']).timestamp.nunique()
    avg_alive_encounter_count = aggregate_living_patients.mean()
    max_alive_encounter_count = aggregate_living_patients.max()
    min_alive_encounter_count = aggregate_living_patients.min()

    return min_dead_encounter_count, max_dead_encounter_count, avg_dead_encounter_count, min_alive_encounter_count, max_alive_encounter_count, avg_alive_encounter_count

def record_length_metrics(events, mortality):
    '''
    Record length is the duration between the first event and the last event for a given patient.
    This function needs to be completed.
    '''
    date_format = "%Y-%m-%d"

    deceased_patients = deceased_patients_on_patient_id(events, mortality)
    deceased_dates = deceased_patients.groupby(['patient_id']).timestamp_x
    max_deceased_date = list(deceased_dates.max())
    min_deceased_date = list(deceased_dates.min())
    deceased_date_difference = []
    for i in range(0, len(min_deceased_date)):
        val = utils.date_convert(max_deceased_date[i]) - utils.date_convert(min_deceased_date[i])
        deceased_date_difference.append(val.days)

    living_patients = patients_not_in_deceased(events, deceased_patients)
    alive_dates = living_patients.groupby(['patient_id']).timestamp
    max_alive_date = list(alive_dates.max())
    min_alive_date = list(alive_dates.min())

    alive_date_difference = []
    for i in range(0, len(min_alive_date)):
        val = utils.date_convert(max_alive_date[i]) - utils.date_convert(min_alive_date[i])
        alive_date_difference.append(val.days)

    avg_dead_rec_len = sum(deceased_date_difference)/float(len(deceased_date_difference))

    max_dead_rec_len = max(deceased_date_difference)

    min_dead_rec_len = min(deceased_date_difference)

    avg_alive_rec_len = sum(alive_date_difference)/float(len(alive_date_difference))

    max_alive_rec_len = max(alive_date_difference)

    min_alive_rec_len = min(alive_date_difference)

    return min_dead_rec_len, max_dead_rec_len, avg_dead_rec_len, min_alive_rec_len, max_alive_rec_len, avg_alive_rec_len

def main():
    '''
    DONOT MODIFY THIS FUNCTION.
    Just update the train_path variable to point to your train data directory.
    '''
    #Modify the filepath to point to the CSV files in train_data
    train_path = '../data/train/'
    events, mortality = read_csv(train_path)

    #Compute the event count metrics
    start_time = time.time()
    event_count = event_count_metrics(events, mortality)
    end_time = time.time()
    print("Time to compute event count metrics: " + str(end_time - start_time) + "s")
    print event_count

    #Compute the encounter count metrics
    start_time = time.time()
    encounter_count = encounter_count_metrics(events, mortality)
    end_time = time.time()
    print("Time to compute encounter count metrics: " + str(end_time - start_time) + "s")
    print encounter_count

    #Compute record length metrics
    start_time = time.time()
    record_length = record_length_metrics(events, mortality)
    end_time = time.time()
    print("Time to compute record length metrics: " + str(end_time - start_time) + "s")
    print record_length

if __name__ == "__main__":
    main()
