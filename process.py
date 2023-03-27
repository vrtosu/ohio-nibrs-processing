import pandas as pd
import time
import matplotlib.pyplot as plt
import logging
import lookups, constants

data = pd.DataFrame()
max_age_data = pd.DataFrame()
merged_df = pd.DataFrame()


logging.basicConfig(format='%(asctime)s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S', level=logging.INFO)

def load_input():
    global data

    logging.info(f"Loading tabs {constants.incident_tabname} and {constants.offender_tabname} from file {constants.file}")
    logging.info(">>> This takes about 45 seconds <<<")
    start_time = time.time()
    data = pd.read_excel(constants.file, sheet_name=constants.tab_names)
    time_taken = time.time() - start_time
    logging.info(f"Loaded tabs in {time_taken:.4f} seconds")
    logging.info(constants.end_line)


def numeric_stats(field_name, tab_name):
    logging.info(
        f"Computing numeric stats for [{field_name}] in tab [{tab_name}] stats now\n")

    global data

    filtered = pd.to_numeric(
        data[tab_name][field_name], errors='coerce').dropna()

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


def descriptive_stats():
    logging.info('Computing descriptive stats now')

    global data

    logging.info(
        f"Total incidents with data in {constants.offender_tabname}: {data[constants.offender_tabname]['incident_id'].nunique()}")
    logging.info(
        f"Total incidents in {constants.incident_tabname}: {data[constants.incident_tabname]['incident_id'].nunique()}")

    # numeric_stats('incident_month', incident_tabname)
    numeric_stats('age_num', constants.offender_tabname)
    numeric_stats('age_range_low_num', constants.offender_tabname)
    numeric_stats('age_range_high_num', constants.offender_tabname)

    logging.info(constants.end_line)


def merge_dataframes():
    logging.info('Computing a merged dataframe now')

    global data, max_age_data, merged_df

    race_lookup_df = pd.DataFrame({'race_id': list(
        lookups.race_lookup_dict.keys()), 'race_desc': list(lookups.race_lookup_dict.values())})
    ethnicity_lookup_df = pd.DataFrame({'ethnicity_id': list(
        lookups.ethnicity_lookup_dict.keys()), 'ethnicity_desc': list(lookups.ethnicity_lookup_dict.values())})

    start_time = time.time()

    logging.info(
        'Merging incident and offender dfs using maximum offender_seq_nbr now')

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

    merged_df['incident_date'] = pd.to_datetime(merged_df['incident_date'])
    merged_df['incident_day'] = merged_df['incident_date'].dt.day
    merged_df['incident_month'] = merged_df['incident_date'].dt.month
    merged_df['incident_year'] = merged_df['incident_date'].dt.year

    merged_df = pd.concat(
        [merged_df, merged_df[['incident_day', 'incident_month', 'incident_year']]], axis=1)

    time_taken = time.time() - start_time

    logging.info(
        f"Merged data frame computed in {time_taken:.4f} seconds; writing merged DF to disk")

    # merged_df.to_excel(merged_file, index=False, sheet_name='Appended')

    logging.info(constants.end_line)

def process_a_correlation(col1, col2):
    global merged_df

    start_time = time.time()

    filter_merged_df = pd.to_numeric(
        merged_df[col1], errors='coerce').dropna()
    
    filter_merged_df = pd.to_numeric(
        merged_df[col2], errors='coerce').dropna()

    correlation = filter_merged_df[col1].corr(
        filter_merged_df[col2])

    time_taken = time.time() - start_time

    logging.info(
        f"The correlation between {col1} and {col2} is {correlation}; completed in {time_taken:.4f} seconds")
    
    logging.info(constants.end_line)


def process_correlations():
    logging.info('Computing correlations now')

    for key, value in lookups.correlation_dict.items():
        print(f"Now processing {key}-{value}")
        process_a_correlation(key, value)
    """
    total_rows = merged_df.shape[0]

    filter_merged_df = pd.to_numeric(
        merged_df['age_num'], errors='coerce').dropna()

    logging.info(
        f"Identified {total_rows - len(filter_merged_df)} rows with non-numeric values for age_id; need to be skipped for correlation")

    start_time = time.time()

    correlation1 = merged_df['incident_hour'].corr(merged_df['age_id'])

    time_taken = time.time() - start_time

    logging.info(
        f"The correlation between incident_hour and age_id is {correlation1} in {time_taken:.4f} seconds")

    start_time = time.time()

    correlation2 = merged_df['incident_hour'].corr(
        merged_df['offender_seq_num'])

    time_taken = time.time() - start_time

    logging.info(
        f"The correlation between incident_hour and max offender_seq_num (indicating total # of perps) is {correlation2} in {time_taken:.4f} seconds")
    """
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
descriptive_stats()
merge_dataframes()
process_correlations()
# histograms()
process_offender_by_race()
process_offender_by_gender()
process_offender_by_ethnicity()
