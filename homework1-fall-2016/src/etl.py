import utils
import time
import numpy as n
import pandas as p
from datetime import datetime, timedelta

def read_csv(filepath):

    '''
    TODO: This function needs to be completed.
    Read the events.csv, mortality_events.csv and event_feature_map.csv files into events, mortality and feature_map.

    Return events, mortality and feature_map
    '''

    #Columns in events.csv - patient_id,event_id,event_description,timestamp,value
    events = p.read_csv(filepath + 'events.csv')

    #Columns in mortality_event.csv - patient_id,timestamp,label
    mortality = p.read_csv(filepath + 'mortality_events.csv')

    #Columns in event_feature_map.csv - idx,event_id
    feature_map = p.read_csv(filepath + 'event_feature_map.csv')

    return events, mortality, feature_map


def calculate_index_date(events, mortality, deliverables_path):

    '''
    TODO: This function needs to be completed.

    Refer to instructions in Q3 a

    Suggested steps:
    1. Create list of patients alive ( mortality_events.csv only contains information about patients deceased)
    2. Split events into two groups based on whether the patient is alive or deceased
    3. Calculate index date for each patient

    IMPORTANT:
    Save indx_date to a csv file in the deliverables folder named as etl_index_dates.csv.
    Use the global variable deliverables_path while specifying the filepath.
    Each row is of the form patient_id, indx_date.
    The csv file should have a header
    For example if you are using Pandas, you could write:
        indx_date.to_csv(deliverables_path + 'etl_index_dates.csv', columns=['patient_id', 'indx_date'], index=False)

    Return indx_date
    '''

    indx_date = p.DataFrame()

    indx_date['timestamp'] = mortality.timestamp.apply(lambda x:
        (utils.date_convert(x) - timedelta(days=30)).strftime('%Y-%m-%d'))
    indx_date['patient_id'] = mortality['patient_id']

    #negation
    living_patients = events[~events.patient_id.isin(mortality.patient_id)]
    living_patients_timestamp = living_patients.groupby(['patient_id']).agg({'timestamp' : n.max}).reset_index()

    indx_date = p.concat([living_patients_timestamp, indx_date]).reset_index().drop('index', 1)

    indx_date = indx_date.rename(columns={'timestamp': 'indx_date'})

    indx_date['indx_date'] = indx_date.indx_date.apply(lambda x:
     utils.date_convert(x))

    indx_date.to_csv(deliverables_path + 'etl_index_dates.csv',columns=['patient_id', 'indx_date'], index=False)

    return indx_date

def filter_events(events, indx_date, deliverables_path):

    '''
    TODO: This function needs to be completed.

    Refer to instructions in Q3 a

    Suggested steps:
    1. Join indx_date with events on patient_id
    2. Filter events occuring in the observation window(IndexDate-2000 to IndexDate)


    IMPORTANT:
    Save filtered_events to a csv file in the deliverables folder named as etl_filtered_events.csv.
    Use the global variable deliverables_path while specifying the filepath.
    Each row is of the form patient_id, event_id, value.
    The csv file should have a header
    For example if you are using Pandas, you could write:
        filtered_events.to_csv(deliverables_path + 'etl_filtered_events.csv', columns=['patient_id', 'event_id', 'value'], index=False)

    Return filtered_events
    '''

    filtered_events = p.merge(events, indx_date, on='patient_id')
    filtered_events['to-be'] = filtered_events.timestamp.apply(lambda x: utils.date_convert(x))
    filtered_events['bound'] = filtered_events.indx_date.apply(lambda x: x - timedelta(days=2000))
    filtered_events = filtered_events[(filtered_events['to-be'] <= filtered_events['indx_date'])
     & (filtered_events['to-be'] >= filtered_events['bound'])]

    filtered_events.to_csv(deliverables_path + 'etl_filtered_events.csv',
        columns=['patient_id', 'event_id', 'value'], index=False)

    return filtered_events

def aggregate_events(filtered_events_df, mortality_df,feature_map_df, deliverables_path):

    '''
    TODO: This function needs to be completed.

    Refer to instructions in Q3 a

    Suggested steps:
    1. Replace event_id's with index available in event_feature_map.csv
    2. Remove events with n/a values
    3. Aggregate events using sum to calculate feature value
    4. Normalize the values obtained above using min-max normalization


    IMPORTANT:
    Save aggregated_events to a csv file in the deliverables folder named as etl_aggregated_events.csv.
    Use the global variable deliverables_path while specifying the filepath.
    Each row is of the form patient_id, event_id, value.
    The csv file should have a header .
    For example if you are using Pandas, you could write:
        aggregated_events.to_csv(deliverables_path + 'etl_aggregated_events.csv', columns=['patient_id', 'feature_id', 'feature_value'], index=False)

    Return filtered_events
    '''
    combined = p.merge(filtered_events_df, feature_map_df, on='event_id').dropna(subset=['value'])

    combined = combined.groupby(['idx', 'patient_id', 'event_id']).agg({'value': [n.sum, n.mean, len, n.min, n.max]}).reset_index()
    combined['filter'] = combined.event_id.apply(lambda x: x[:3])
    combined['summed'] = combined['value']['sum']
    combined.ix[combined['filter'] == 'LAB', 'summed'] = list(combined.loc[combined['filter'] == 'LAB']['value']['len'])

    aggregated_events = p.DataFrame()
    aggregated_events['feature_id'] = combined['idx']
    aggregated_events['value'] = combined['summed']
    aggregated_events['patient_id'] = combined['patient_id']
    aggregated_events = aggregated_events.rename(columns={'idx': 'feature_id','summed' : 'value'})

    aggregated_events['max_by_feature'] = aggregated_events.groupby('feature_id')['value'].transform(max)
    aggregated_events['feature_value'] = aggregated_events['value'].divide(aggregated_events['max_by_feature'], 0)

    aggregated_events.to_csv(deliverables_path + 'etl_aggregated_events.csv',
        columns=['patient_id', 'feature_id', 'feature_value'], index=False)

    return aggregated_events

def create_features(events, mortality, feature_map):

    deliverables_path = '../deliverables/'

    #Calculate index date
    indx_date = calculate_index_date(events, mortality, deliverables_path)

    #Filter events in the observation window
    filtered_events = filter_events(events, indx_date,  deliverables_path)

    #Aggregate the event values for each patient
    aggregated_events = aggregate_events(filtered_events, mortality, feature_map, deliverables_path)

    '''
    TODO: Complete the code below by creating two dictionaries -
    1. patient_features :  Key - patient_id and value is array of tuples(feature_id, feature_value)
    2. mortality : Key - patient_id and value is mortality label
    '''
    patient_features = {}
    mortality_output = {}

    for i, row in mortality.iterrows():
        mortality_output[float(mortality.ix[i, 'patient_id'])] = float(mortality.ix[i, 'label'])

    for i, row in aggregated_events.iterrows():
        if aggregated_events.ix[i, 'patient_id'] in patient_features:
            patient_features[float(aggregated_events.ix[i, 'patient_id'])].append((float(aggregated_events.ix[i, 'feature_id']),float(aggregated_events.ix[i, 'feature_value'])))
        else:
            patient_features[float(aggregated_events.ix[i, 'patient_id'])] = [(float(aggregated_events.ix[i, 'feature_id']),float(aggregated_events.ix[i, 'feature_value']))]
        if aggregated_events.ix[i, 'patient_id'] not in mortality_output:
            mortality_output[float(aggregated_events.ix[i, 'patient_id'])] = 0

    return patient_features, mortality_output

def save_svmlight(patient_features, mortality, op_file, op_deliverable):

    '''
    TODO: This function needs to be completed
    Create two files:
    1. op_file - which saves the features in svmlight format. (See instructions in Q3d for detailed explanation)
    2. op_deliverable - which saves the features in following format:
       patient_id1 label feature_id:feature_value feature_id:feature_value feature_id:feature_value ...
       patient_id2 label feature_id:feature_value feature_id:feature_value feature_id:feature_value ...

    Note: Please make sure the features are ordered in ascending order, and patients are stored in ascending order as well.
    '''
    deliverable1 = open(op_file, 'w')
    for key in patient_features:
        line = ''
        if mortality[key] == 1.0:
            line +='1 '
        else:
            line += '0 '
        for tup in patient_features[key]:
            line += str(int(tup[0])) + ':' + str("{:.3f}".format(tup[1])) + ' '
        deliverable1.write(line + '\n')
    deliverable1.close()


    deliverable2 = open(op_deliverable, 'w')
    for key in patient_features:
        line = str(int(key)) + ' '
        if mortality[key] == 1.0:
            line +='1.0 '
        else:
            line +='0.0 '
        for tup in sorted(patient_features[key], key=lambda x: x[0]):
            line += str(int(tup[0])) + ':' + str("{:.3f}".format(tup[1])) + ' '
        deliverable2.write(line + '\n')
    deliverable2.close()

def main():
    train_path = '../data/train/'
    events, mortality, feature_map = read_csv(train_path)
    patient_features, mortality = create_features(events, mortality, feature_map)
    save_svmlight(patient_features, mortality, '../deliverables/features_svmlight.train', '../deliverables/features.train')

if __name__ == "__main__":
    main()
