import pandas as pd
import time
import matplotlib.pyplot as plt
import numpy as np
import logging
import lookups
import constants

data = pd.DataFrame()
max_age_data = pd.DataFrame()
merged_df = pd.DataFrame()


logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)


def load_input():
    global data

    logging.info(
        f"Loading tabs {constants.incident_tabname} and {constants.offender_tabname} from file {constants.file}")
    
    logging.info(">>> This takes about 45 seconds <<<")
    
    start_time = time.time()

    # Load Input Excel
    data = pd.read_excel(constants.file, sheet_name=constants.tab_names)
    
    time_taken = time.time() - start_time
    
    logging.info(f"Loaded tabs in {time_taken:.4f} seconds")
    logging.info(constants.end_line)


def numeric_stats(field_name):
    logging.info(
        f"Computing numeric stats for [{field_name}] now\n")

    global merged_df

    # Removing rows where non-numeric values are stored (for ex: NS for age_num)
    filtered = pd.to_numeric(
        merged_df[field_name], errors='coerce').dropna()

    min = filtered.min()
    max = filtered.max()
    range = max - min
    mean = filtered.mean()
    median = filtered.median()
    mode = filtered.mode()
    q1 = filtered.quantile(0.25)
    q2 = filtered.quantile(0.5)
    q3 = filtered.quantile(0.75)
    iqr = q3 - q1

    print(f"\tmin [{min}]\n\tmax [{max}]\n\trange [{range}]\n\tmean [{mean}]\n\tmedian [{median}]\n\tmode [{mode[0]}]\n\tiqr [{iqr}]")

    # Generate and save boxplot
    plt.figure()
    merged_df.boxplot(column=field_name)
    plt.savefig(field_name + '.png')


def descriptive_stats():
    logging.info('Computing descriptive stats now')

    global data

    logging.info(
        f"Total incidents with data in {constants.offender_tabname}: {data[constants.offender_tabname]['incident_id'].nunique()}")
    logging.info(
        f"Total incidents in {constants.incident_tabname}: {data[constants.incident_tabname]['incident_id'].nunique()}")

    # Generate numerical stats for fields in lookups.numeric_stats
    for i in range(len(lookups.numeric_stats)):
        numeric_stats(lookups.numeric_stats[i])

    logging.info(constants.end_line)


def merge_dataframes():
    logging.info('Computing a merged dataframe now')

    global data, max_age_data, merged_df

    # Race id mapping
    race_lookup_df = pd.DataFrame({'race_id': list(
        lookups.race_lookup_dict.keys()), 'race_desc': list(lookups.race_lookup_dict.values())})
    
    # Ethnicity id mapping
    ethnicity_lookup_df = pd.DataFrame({'ethnicity_id': list(
        lookups.ethnicity_lookup_dict.keys()), 'ethnicity_desc': list(lookups.ethnicity_lookup_dict.values())})

    start_time = time.time()

    logging.info(
        'Merging incident and offender dfs using maximum offender_seq_nbr now')

    # Extract highest offender_seq_num for each incident_id -> also gives us # perps etc.
    max_age_data = data[constants.offender_tabname].loc[data[constants.offender_tabname].groupby(
        'incident_id')['offender_seq_num'].idxmax()]
    merged_df = data[constants.incident_tabname].merge(
        max_age_data, on='incident_id', how='left')

    logging.info('Adding race description now')

    merged_df = pd.merge(merged_df, race_lookup_df, on='race_id', how='left')

    logging.info('Adding ethnicity description now')

    merged_df = pd.merge(merged_df, ethnicity_lookup_df,
                         on='ethnicity_id', how='left')

    logging.info('Breaking out incident_date into day and month now')
    
    # Breakout incident_date into day, month and year

    merged_df['incident_date'] = pd.to_datetime(merged_df['incident_date'])
    merged_df['incident_day'] = merged_df['incident_date'].dt.day
    merged_df['incident_month'] = merged_df['incident_date'].dt.month
    merged_df['incident_year'] = merged_df['incident_date'].dt.year

    merged_df['submission_date'] = pd.to_datetime(merged_df['submission_date'])

    # Number of days between incident date and report date
    merged_df['incident_to_submission_days'] = (merged_df['submission_date'] - merged_df['incident_date']).dt.days

    merged_df['age_num'] = merged_df['age_num'].replace('NS', np.nan)

    merged_df = pd.concat(
        [merged_df, merged_df[['incident_day', 'incident_month', 'incident_year']]], axis=1)

    time_taken = time.time() - start_time

    logging.info(
        f"Merged data frame computed in {time_taken:.4f} seconds; writing merged DF to disk")

    # merged_df.to_excel(constants.merged_file,
    #                   index=False, sheet_name='Appended')

    logging.info(constants.end_line)


def process_a_correlation(col1, col2):
    global merged_df

    start_time = time.time()

    # Pearson's Coeff
    merged_df[col1] = pd.to_numeric(merged_df[col1], errors='coerce')
    merged_df = merged_df[pd.notnull(merged_df[col1])]

    merged_df[col2] = pd.to_numeric(merged_df[col2], errors='coerce')
    merged_df = merged_df[pd.notnull(merged_df[col2])]

    correlation = merged_df[col1].corr(merged_df[col2])

    time_taken = time.time() - start_time

    logging.info(
        f"The correlation between {col1} and {col2} is {correlation}; completed in {time_taken:.4f} seconds")

    logging.info(constants.end_line)


def process_correlations():
    logging.info('Computing correlations now')

    for i in range(len(lookups.correlation_dict)):
        key = lookups.correlation_dict[i][0]
        value = lookups.correlation_dict[i][1]

        print(f"Now processing {key}-{value}")

        process_a_correlation(key, value)
    
    logging.info(constants.end_line)


def histograms():
    logging.info('Computing histograms now')

    global data

    merged_df['incident_hour'].hist()

    plt.show()

    logging.info(constants.end_line)


def process_offender_by_race():
    global data, merged_df
    ethnicity_counts = merged_df['race_desc'].value_counts()
    print(f"Race counts\n{ethnicity_counts}")
    logging.info(constants.end_line)


def process_offender_by_gender():
    global data, merged_df
    ethnicity_counts = merged_df['sex_code'].value_counts()
    print(f"Sex counts\n{ethnicity_counts}")
    logging.info(constants.end_line)


def process_offender_by_ethnicity():
    global data, merged_df
    ethnicity_counts = merged_df['ethnicity_desc'].value_counts()
    print(f"Ethnicity counts\n{ethnicity_counts}")
    logging.info(constants.end_line)


load_input()
merge_dataframes()
descriptive_stats()
process_correlations()
# histograms()
process_offender_by_race()
process_offender_by_gender()
process_offender_by_ethnicity()
