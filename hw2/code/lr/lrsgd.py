#!/usr/bin/env python
"""
Implement your own version of logistic regression with stochastic
gradient descent.

Author: 
Email : 
"""

import collections
import math
import sys 


class LogisticRegressionSGD:

    def __init__(self, eta, mu, n_feature):
        """
        Initialization of model parameters  
        """
        self.eta = eta
        self.weight = [0.0] * n_feature
        self.mu = mu

    def fit(self, X, y):
        """
        Update model using a pair of training sample
        """
        W = abs(self.weight[self.idxs(X)])**2
        self.weight[self.idxs(X)] = self.weight[self.idxs(X)] + self.eta * ((y - self.predict_prob(X)) * self.values(X) - self.mu * W)
       

    def predict(self, X):
        return 1 if self.predict_prob(X) > 0.5 else 0

    def predict_prob(self, X):
        return 1.0 / (1.0 + math.exp(-math.fsum((self.weight[f]*v for f, v in X))))

    def values(self, X):
        for f, v in X:
            return v

    def idxs(self, X):
        for f, v in X:
            return f

    def y(self, y):
        return y       
