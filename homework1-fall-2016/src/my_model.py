import utils
import numpy as n
import pandas as p
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
#Note: You can reuse code that you wrote in etl.py and models.py and cross.py over here. It might help.

'''
You may generate your own features over here.
Note that for the test data, all events are already filtered such that they fall in the observation window of their respective patients. Thus, if you were to generate features similar to those you constructed in code/etl.py for the test data, all you have to do is aggregate events for each patient.
IMPORTANT: Store your test data features in a file called "test_features.txt" where each line has the
patient_id followed by a space and the corresponding feature in sparse format.
Eg of a line:
60 971:1.000000 988:1.000000 1648:1.000000 1717:1.000000 2798:0.364078 3005:0.367953 3049:0.013514
Here, 60 is the patient id and 971:1.000000 988:1.000000 1648:1.000000 1717:1.000000 2798:0.364078 3005:0.367953 3049:0.013514 is the feature for the patient with id 60.

Save the file as "test_features.txt" and save it inside the folder deliverables

input:
output: X_train,Y_train,X_test
'''
def my_features():
    #TODO: complete this
    X_train, Y_train = utils.get_data_from_svmlight("../deliverables/features_svmlight.train")
    test_events = p.read_csv('../data/test/events.csv')
    event_feature_map = p.read_csv('../data/test/event_feature_map.csv')
    aggregate_for_each_patient(test_events, event_feature_map)
    X_test, Y_test = utils.get_data_from_svmlight("../data/test/feature_svmlight.test")
    return X_train,Y_train,X_test

def aggregate_for_each_patient(test_events, event_feature_map):
    #copy/pasting from etl.py
    combined = p.merge(test_events, event_feature_map, on='event_id').dropna(subset=['value'])
    combined = combined.groupby(['idx', 'patient_id', 'event_id']).agg({'value': [n.sum, n.mean, len, n.min, n.max]}).reset_index()

    aggregated_events = p.DataFrame()
    aggregated_events['feature_id'] = combined['idx']
    aggregated_events['value'] = combined['value']['sum']
    aggregated_events['patient_id'] = combined['patient_id']
    aggregated_events = aggregated_events.rename(columns={'idx': 'feature_id','summed' : 'value'})
    aggregated_events['max_by_feature'] = aggregated_events.groupby('feature_id')['value'].transform(max)
    aggregated_events['feature_value'] = aggregated_events['value'].divide(aggregated_events['max_by_feature'], 0)

    patient_features = {}
    for i, row in aggregated_events.iterrows():
        if aggregated_events.ix[i, 'patient_id'] in patient_features:
            patient_features[float(aggregated_events.ix[i, 'patient_id'])].append((float(aggregated_events.ix[i, 'feature_id']),float(aggregated_events.ix[i, 'feature_value'])))
        else:
            patient_features[float(aggregated_events.ix[i, 'patient_id'])] = [(float(aggregated_events.ix[i, 'feature_id']),float(aggregated_events.ix[i, 'feature_value']))]
        # if aggregated_events.ix[i, 'patient_id'] not in mortality_output:
        #     mortality_output[float(aggregated_events.ix[i, 'patient_id'])] = 0

    deliverable1 = open('../data/test/feature_svmlight.test', 'w')
    for key in patient_features:
		#this placeholder is needed to read the data properly
        line = '1 '
        # if mortality[key] == 1.0:
        # line +='1 '
        # else:
            # line += '0 '
        for tup in patient_features[key]:
            line += str(int(tup[0])) + ':' + str("{:.6f}".format(tup[1])) + ' '
        deliverable1.write(line + '\n')
    deliverable1.close()

    deliverable2 = open('../deliverables/test_features.txt', 'w')
    for key in patient_features:
        line = str(int(key)) + ' '
        # if mortality[key] == 1.0:
            # line +='1.0 '
        # else:
            # line +='0.0 '
        for tup in sorted(patient_features[key], key=lambda x: x[0]):
            line += str(int(tup[0])) + ':' + str("{:.3f}".format(tup[1])) + ' '
        deliverable2.write(line + '\n')
    deliverable2.close()


'''
You can use any model you wish.

input: X_train, Y_train, X_test
output: Y_pred
'''
def my_classifier_predictions(X_train,Y_train,X_test):
    #TODO: complete this
	lr = LogisticRegression(random_state=545510477)
	lr.fit(X_train, Y_train)
	return lr.predict(X_test)


def main():
    X_train, Y_train, X_test = my_features()
    Y_pred = my_classifier_predictions(X_train,Y_train,X_test)
    # print Y_pred
    utils.generate_submission("../deliverables/test_features.txt",Y_pred)
    #The above function will generate a csv file of (patient_id,predicted label) and will be saved as "my_predictions.csv" in the deliverables folder.

if __name__ == "__main__":
    main()
