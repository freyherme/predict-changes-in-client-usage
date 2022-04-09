import pickle
from sklearn.calibration import calibration_curve
from sklearn.calibration import CalibratedClassifierCV
from matplotlib import pyplot
import pandas as pd

#####################################
#            Load Model             #
#####################################
svm_model = pickle.load(open('svm_full_cv_model_obj.sav', 'rb'))

#####################################
#           Import Data             #
#####################################

# Import model & model_input data
client_health_data = pd.read_pickle(f'client_health_df.pkl')
y = client_health_data['hs_did_change']
X = client_health_data.drop('hs_did_change', 1)

########################################
#   Pre-Calibrated Reliability Curve   #
########################################
print("Generating pre-calibrated reliability curve...")
# predict probabilities
probs = svm_model.decision_function(X)
# reliability diagram
fop, mpv = calibration_curve(y, probs, n_bins = 10, normalize = True)
# plot perfectly calibrated
pyplot.plot([0, 1], [0, 1], linestyle = '--')
# plot model reliability
pyplot.plot(mpv, fop, marker = '.')
pyplot.savefig(f'svm_pre-callibration_reliability_diagram.png')
pyplot.show()

########################################
#     Calibrate Model Probabilities    #
########################################
print("Calibrating model...")
calibrated_svm_model = CalibratedClassifierCV(svm_model, method = 'sigmoid', cv = 5, n_jobs = 15)
calibrated_svm_model.fit(X, y)

#########################################
#   Post-Calibrated Reliability Curve   #
#########################################
print("Generating post-calibration probabilities...")
# predict probabilities
cb_probs = calibrated_svm_model.predict_proba(X)[:, 1]
# reliability diagram
print("Generating post-calibration reliability curve...")
cb_fop, cb_mpv = calibration_curve(y, cb_probs, n_bins = 10, normalize = True)
# plot perfectly calibrated
pyplot.plot([0, 1], [0, 1], linestyle = '--')
# plot model reliability
pyplot.plot(cb_mpv, cb_fop, marker = '.')
pyplot.savefig(f'svm_post-callibration_reliability_diagram_clean.png')
pyplot.show()

model_filename = f"svm_calibrated_model_obj.sav"
pickle.dump(calibrated_svm_model, open(model_filename, 'wb'))

print("fin.")
