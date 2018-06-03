#! /usr/bin/python

import pandas as pd
import seaborn as sb
import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.svm import SVC
from sklearn.neural_network import MLPClassifier
from sklearn.naive_bayes import GaussianNB


mydata_train = pd.read_csv('dfX.csv')

features = ['country', 'os', 'access', 'device_type', 'gender']


#create randomized test data_set
r = 10**6
mydata_test = pd.DataFrame(index=range(r), columns=features)
mydata_test['os'] = list(mydata.os.sample(r))
mydata_test['access'] = list(mydata.access.sample(r))
mydata_test['gender'] = list(mydata.gender.sample(r))
mydata_test['country'] = list(mydata.country.sample(r))
mydata_test['device_type'] = list(mydata.device_type.sample(r))



#mydata_train, mydata_test = train_test_split(mydata, random_state=0, test_size=.2)



#Standardize features by removing the mean and scaling to unit variance
scaler = StandardScaler()
scaler.fit(mydata_train.ix[:,features]) 
X_train = scaler.transform(mydata_train.ix[:,features])
X_test = scaler.transform(mydata_test.ix[:,features])


y_train = list(mydata_train['label'].values)
y_test = list(mydata_test['label'].values)























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




















'''
import warnings; warnings.simplefilter('ignore')
sb.set_style("whitegrid", {'axes.grid' : False})

high_nfsc = mydata.loc[mydata.label==1]
low_nfsc = mydata.loc[mydata.label==0]
#take a sample of the low_nfsc population of equal size of the high_nfsc (otherwise the frequencies will be very uneven)
low_nfsc_ = low_nfsc.sample(len(high_nfsc))

features = ['country', 'os', 'access', 'device_type', 'gender']

#Plot the histograms
fig, axes = plt.subplots(3, 2, figsize=(20,30))

ax = axes.flatten() #ravel()

for i in range(len(features)):
    #ax[i].hist(low_nfsc_.ix[:,features[i]], bins=20, color='blue', alpha=.5)
    sb.countplot(low_nfsc_.ix[:,features[i]], alpha=.5, ax = ax[i])
    #ax[i].hist(high_nfsc.ix[:, features[i]], bins=20, color='red', alpha=.5)
    sb.countplot(high_nfsc.ix[:,features[i]], alpha=.5, ax = ax[i])
    ax[i].set_title(features[i], fontsize=12)
    ax[i].set_yticks(())
    
ax[0].set_xlabel("Feature magnitude")
ax[0].set_ylabel("Frequency")
ax[0].legend(["low nfsc", "high nfsc"], loc="best")
fig.tight_layout()

plt.show()
'''






