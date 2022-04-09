
import pandas as pd
from numpy import arange
from numpy import argmax
from sklearn import metrics
import scikitplot as skplt
import matplotlib.pyplot as plt
import pickle


# apply threshold to probabilities to create pred labels
def to_labels(pos_probs, threshold):
    return (pos_probs >= threshold).astype('int')


# Import model & model_input data
client_health_data = pd.read_pickle(f'client_health_df.pkl')
y = client_health_data['hs_did_change']
X = client_health_data.drop('hs_did_change', 1)

model_obj = pickle.load(open('model.sav', 'rb'))

# Predict
preds = model_obj.predict(X)
predicted_probs = model_obj.predict_proba(X)

# "No Skill" predictions to draw line in ROC curve
ns_probs = [0 for _ in range(len(y))]
lr_probs = predicted_probs[:, 1]

ns_auc_score = metrics.roc_auc_score(y, ns_probs)
auc_score = metrics.roc_auc_score(y, lr_probs)
print(f"auc_score: {round(auc_score, 4)}")

f1_score = metrics.f1_score(y, preds)
print(f"f1_score: {f1_score}")

# Calculate ROC
ns_fpr, ns_tpr, _ = metrics.roc_curve(y, ns_probs)
lr_fpr, lr_tpr, _ = metrics.roc_curve(y, lr_probs)

# Plot Simple ROC
plt.plot(ns_fpr, ns_tpr, linestyle = '--', label = 'No Skill')
plt.plot(lr_fpr, lr_tpr, marker = '.', label = 'Model')

# axis labels
plt.xlabel('False Positive Rate')
plt.ylabel('True Positive Rate')

# show the legend
plt.legend()

# show the plot
plt.savefig(f'simple_auc_plot.png')
plt.show()

# Plot multi-class ROC
skplt.metrics.plot_roc(y, predicted_probs)
multiclass_filename = f'multiclass_auc_plot.png'
plt.savefig(multiclass_filename)
plt.show()

##########################
#    Threshold Tuning    #
##########################

# DEFINE THRESHOLDS
thresholds = arange(0, 1, 0.01)
# EVALUATE EACH THRESHOLD
scores = [metrics.f1_score(y, to_labels(lr_probs, t)) for t in thresholds]
# GET BEST THRESHOLD
ix = argmax(scores)
print('Threshold=%.3f, F-Score=%.5f' % (thresholds[ix], scores[ix]))

best_threshold = thresholds[ix]
best_threshold_preds = [1 if prob > best_threshold else 0 for prob in lr_probs]

best_threshold_accuracy = round(metrics.accuracy_score(y, best_threshold_preds), 4)
best_threshold_auc = round(metrics.roc_auc_score(y, best_threshold_preds), 4)
best_threshold_f1 = round(metrics.f1_score(y, best_threshold_preds), 4)
best_threshold_precision = round(metrics.precision_score(y, best_threshold_preds), 4)
best_threshold_recall = round(metrics.recall_score(y, best_threshold_preds), 4)

feature_names = model_obj[:-1].get_feature_names_out()
for i, coef in enumerate(model_obj.steps[1][1].coef_[0]):
    print(f"{feature_names[i]}: {coef}")
