import models
from sklearn.cross_validation import KFold, ShuffleSplit
from numpy import mean

import utils

#input: training data and corresponding labels
#output: accuracy, auc
def get_acc_auc_kfold(X,Y,k=5):
	#TODO:First get the train indices and test indices for each iteration
	#Then train the classifier accordingly
	#Report the mean accuracy and mean auc of all the folds
	acc = []
	auc = []

	kf = KFold(n=X.get_shape()[0], n_folds=k, random_state=545510477)
	for train_index, test_index in kf:
		X_train, X_test = X[train_index], X[test_index]
		Y_train, Y_test = Y[train_index], Y[test_index]
		Y_pred = models.logistic_regression_pred(X_train, Y_train, X_test)
		_acc, _auc, p, r, f = models.classification_metrics(Y_pred,Y_test)
		acc.append(_acc)
		auc.append(_auc)
	return sum(acc)/len(acc),sum(auc)/len(auc)

#input: training data and corresponding labels
#output: accuracy, auc
def get_acc_auc_randomisedCV(X,Y,iterNo=5,test_percent=0.2):
	#TODO: First get the train indices and test indices for each iteration
	#Then train the classifier accordingly
	#Report the mean accuracy and mean auc of all the iterations
	acc = []
	auc = []
	ss = ShuffleSplit(X.get_shape()[0], n_iter=iterNo, test_size=test_percent,random_state=545510477)
	for train_index, test_index in ss:
		X_train, X_test = X[train_index], X[test_index]
		Y_train, Y_test = Y[train_index], Y[test_index]
		Y_pred = models.logistic_regression_pred(X_train, Y_train, X_test)
		_acc, _auc, p, r, f = models.classification_metrics(Y_pred,Y_test)
		acc.append(_acc)
		auc.append(_auc)

	return sum(acc)/len(acc),sum(auc)/len(auc)


def main():
	X,Y = utils.get_data_from_svmlight("../deliverables/features_svmlight.train")
	print "Classifier: Logistic Regression__________"
	acc_k,auc_k = get_acc_auc_kfold(X,Y)
	print "Average Accuracy in KFold CV: "+str(acc_k)
	print "Average AUC in KFold CV: "+str(auc_k)
	acc_r,auc_r = get_acc_auc_randomisedCV(X,Y)
	print "Average Accuracy in Randomised CV: "+str(acc_r)
	print "Average AUC in Randomised CV: "+str(auc_r)

if __name__ == "__main__":
	main()
