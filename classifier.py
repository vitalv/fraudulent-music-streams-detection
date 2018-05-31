#! /usr/bin/python

from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.svm import SVC
from sklearn.neural_network import MLPClassifier
from sklearn.naive_bayes import GaussianNB


X = train_df.iloc[:,range(0,10)]
y = train_df.label.values



#Train support vector machine model
svm = SVC(kernel='linear') #linear kernel seems to perform better than default 'rbf'
svm.fit(X, y)
print("\nSupport Vector Machine")
print("Accuracy on training set: {:.3f}".format(svm.score(X, y)))
#Prediction using SVM classifier
svm.predict(test_df)
#Re-write test data set including the assigned class
test_df['label'] = svm.predict(test_df)
test_df.to_csv('test_data_svm_classes.csv')
#Drop the label column to continue with the other classfiers
test_df = test_df.drop('label', 1)


#Train decision tree model
tree = DecisionTreeClassifier(random_state=0).fit(X, y)
print("\nDecision Tree")
print("Accuracy on training set: {:.3f}".format(tree.score(X, y)))
#Prediction using Decission Tree
tree.predict(test_df)
#Re-write test data set including the assigned class
test_df['label'] = tree.predict(test_df)
test_df.to_csv('test_data_tree_classes.csv')
test_df = test_df.drop('label', 1)


#Train Naive Bayes classifier
nb = GaussianNB()
nb.fit(X,y)
print("\nNaive Bayes")
print("Accuracy on training set: {:.3f}".format(nb.score(X, y)))
#Prediction using NB classifier
nb.predict(test_df)
#Re-write test data set including the assigned class
test_df['label'] = tree.predict(test_df)
test_df.to_csv('test_data_nb_classes.csv')
test_df = test_df.drop('label', 1)


#Train random forest model
forest = RandomForestClassifier(n_estimators=5, random_state=0).fit(X, y)
print("\nRandom Forests")
print("Accuracy on training set: {:.3f}".format(forest.score(X, y)))
#Prediction using Random Forest Classifier
forest.predict(test_df)
#Re-write test data set including the assigned class
test_df['label'] = forest.predict(test_df)
test_df.to_csv('test_data_forest_classes.csv')
test_df = test_df.drop('label', 1)


#Train gradient boosting model
gbrt = GradientBoostingClassifier(random_state=0).fit(X, y)
print("\nGradient Boosting")
print("Accuracy on training set: {:.3f}".format(gbrt.score(X, y)))
#Prediction using Gradient Boosting
gbrt.predict(test_df)
#Re-write test data set including the assigned class
test_df['label'] = gbrt.predict(test_df)
test_df.to_csv('test_data_nb_classes.csv')
test_df = test_df.drop('label', 1)