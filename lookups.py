race_lookup_dict = {10: 'White', 20: 'Black or African American', 30: 'American Indian or Alaska Native',
                    40: 'Asian', 50: 'Native Hawaiian or Other Pacific Islander', 98: 'Unknown', 99: 'Multiple races'}

ethnicity_lookup_dict = {20: 'Hispanic or Latino',
                         40: 'Not Hispanic or Latino', 10: 'Unknown'}

correlation_dict = [['incident_hour', 'age_num'], ['incident_hour', 'offender_seq_num'], [
    'incident_day', 'age_num'], ['incident_day', 'offender_seq_num'], [
    'incident_month', 'age_num'], ['incident_month', 'offender_seq_num']]

numeric_stats_keys = ['age_num', 'age_range_low_num',
                      'age_range_high_num', 'incident_to_submission_days']

histograms_keys = ['incident_hour', 'offender_id', 'incident_day',
                   'incident_month', 'race_desc', 'sex_code', 'ethnicity_desc', 'cleared_except_id', 'report_date_flag', 'cargo_theft_flag']
