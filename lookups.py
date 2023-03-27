race_lookup_dict = {10: 'White', 20: 'Black or African American', 30: 'American Indian or Alaska Native',
                    40: 'Asian', 50: 'Native Hawaiian or Other Pacific Islander', 98: 'Unknown', 99: 'Multiple races'}

ethnicity_lookup_dict = {20: 'Hispanic or Latino',
                         40: 'Not Hispanic or Latino', 10: 'Unknown'}

correlation_dict = [['incident_hour', 'age_num'], ['incident_hour', 'offender_seq_num']]

numeric_stats = ['age_num', 'age_range_low_num', 'age_range_high_num', 'incident_to_submission_days']