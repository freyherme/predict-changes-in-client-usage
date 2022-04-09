# predict-changes-in-client-usage
A predictive model, similar to churn detection, that identifies clients whose app usage (i.e. "client health") is most likely to drop below critical thresholds within 3 months.

## Business Objective:
<p style="color:#1e90ff">
Reduce churn and improve customer service at scale by programmatically alerting stakeholders of accounts most likely to become “unhealthy” as measured by an index of usage statistics.
</p>

## Skills Demonstrated:

### Machine Learning Techniques
- Training models on highly imbalanced datasets
  - Implementing cost-sensitive / weighted loss functions
  - Sampling techniques: under-sampling & over-sampling
  - Strategically balancing precision and recall to meet business / end-user needs
- Support Vector Machines
  - Calibrating SVMs to output predicted class probabilities that effectively reflect the true likelihood
- Logistic Regression
- Hyperparameter Tuning
- Rare-event / anomaly detection
- Model evaluation using cross-validation


### Data Collection, Feature Engineering, Data Cleaning  
- SQL (PostgreSQL)
  - Recurssive SQL queries to calculate client tenure
    - Client tenure: how long has the client had a continuous subscription?
  - Aggregate Functions: Window Functions, Grouping
  - CTEs, sub-queries
  - Complex joins
  - Working with DB schemas with many hundreds of tables
- Parallel Processing / Multi-processing
  - Creating multi-processing pools and managers to spawn and coordinate multiple python processes simultaneously
- Data collected from over 800 different client databases across over 40 servers
- Leveraging extensive subject matter expertise to engineer highly predictive features
- Extensive data cleaning and validation

### Python Libraries & Cloud Environments
- AWS RDS
- scikit-learn
- numpy
- pandas
- multiprocessing
- matplotlib
- seaborn
- pickle

