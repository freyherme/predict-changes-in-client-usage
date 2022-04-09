##############################################################################
#                              SVM Parameter Tuning                          #
##############################################################################

#####################################
#              Packages             #
#####################################

from datetime import datetime
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn import svm
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from imblearn.pipeline import Pipeline
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import StratifiedKFold
import pickle

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

#####################################
#          Build Pipeline           #
#####################################

model_dict = {
        "numeric_features": ['has_ise', 'integration_educlimber', 'integration_fast', 'integration_google_classroom',
                             'integration_pra', 'asmt_admin_flex', 'asmt_admin_ib', 'asmt_admin_inspect_prebuilt',
                             'cumulative_common_assessment_count_per_ay', 'hs_1_lag_band', 'hs_1year_lag_band',
                             'hs_4_lag_band', 'hs_5_lag_band', 'hs_6_lag_band', 'hs_7_lag_band', 'hs_band',
                             'matrix_distinct_users', 'mltp_asmt_smry_distinct_users',
                             'mltp_asmt_smry_times_accessed', 'rsp_freq_distinct_users',
                             'site_asmt_ovr_distinct_users',
                             'skills_letter_distinct_users', 'subscriber_tenure_days',
                             'summary_asmt_created', 'ticket_count', 'tile_layouts_created_modified',
                             'user_created_custom_reports', 'arr_dna',
                             'atd_students_assessed_percent', 'atd_users_login_percent',
                             'sa_tenure_in_days', 'teachers_login_percent'],
        "categorical_features": ['csm_name', 'state']
    }


numeric_pipeline = Pipeline(steps = [
            ('scaler', StandardScaler())
        ])

categorical_pipeline = Pipeline(steps = [
    ('onehot', OneHotEncoder(handle_unknown = 'ignore'))
])

svm_obj = svm.SVC(cache_size = 4000, random_state = 1)

preprocessor = ColumnTransformer(
    transformers = [
        ('num', numeric_pipeline, model_dict['numeric_features']),
        ('cat', categorical_pipeline, model_dict['categorical_features'])
    ],
    n_jobs = 15
)

model = Pipeline([
    ('preprocessor', preprocessor),
    ('svm_obj', svm_obj)
])

# foo = preprocessor.fit_transform(X)

#####################################
#         Set Search Grid           #
#####################################

param_grid = [{
    'svm_obj__C': [0.1, 0.2, 0.25],
    'svm_obj__kernel': ['rbf'],
    'svm_obj__class_weight': [{0: 20.0, 1: 80.0}, {0: 25.0, 1: 75.0}, {0: 30.0, 1: 70.0}]
}]

#####################################
#        Grid Search w/ CV          #
#####################################

k = StratifiedKFold(n_splits = 10, shuffle = True, random_state = 19)
svm_grid_search = GridSearchCV(model,
                               param_grid = param_grid,
                               scoring = 'f1',
                               n_jobs = 15,
                               cv = k,
                               pre_dispatch = "n_jobs",
                               refit = False,
                               verbose = 4)

svm_grid_search.fit(X, y)

print(f"svm_grid_search.best_params_: {svm_grid_search.best_params_}")
print(f"svm_grid_search.best_score: {svm_grid_search.best_score_}")
print(f"svm_grid_search.param_grid: {svm_grid_search.param_grid}")
print("fin.")
