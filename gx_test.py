import os
import great_expectations as gx
import re
import pandas as pd
import html
import sys

# CLEAN_FILE_PATH = "/Users/fatemehahmadi/Documents/matelda-deequ/rein_fixed/nasa/clean.csv"
# DIRTY_FILE_PATH = "/Users/fatemehahmadi/Documents/matelda-deequ/rein_fixed/nasa/dirty.csv"


def value_normalizer(value):
    """
    This method takes a value and minimally normalizes it.
    """
    if isinstance(value, str):
        value = html.unescape(value)
        value = re.sub("[\t\n ]+", " ", value, re.UNICODE)
        value = value.strip("\t\n ")
    return value

def read_csv_dataset(dataset_path):
    """
    This method reads a dataset from a csv file path.
    """
    dataframe = pd.read_csv(dataset_path, sep=",", header="infer", encoding="utf-8", dtype=str,
                                keep_default_na=False, low_memory=False).applymap(value_normalizer)
    return dataframe

def get_dataframes_difference(dataframe_1, dataframe_2):
    """
    This method compares two dataframes and returns the different cells.
    """
    if dataframe_1.shape != dataframe_2.shape:
        sys.stderr.write("Two compared datasets do not have equal sizes!\n")
    difference_dictionary = {}
    difference_dataframe = dataframe_1.where(dataframe_1.values != dataframe_2.values).notna()
    for j in range(dataframe_1.shape[1]):
        for i in difference_dataframe.index[difference_dataframe.iloc[:, j]].tolist():
            difference_dictionary[(i, j)] = (dataframe_1.iloc[i, j], dataframe_2.iloc[i, j])
    return difference_dictionary


def process_table(DIRTY_FILE_PATH, CLEAN_FILE_PATH):
    dirty_df = read_csv_dataset(DIRTY_FILE_PATH)
    clean_df = read_csv_dataset(CLEAN_FILE_PATH)
    diff_dict = get_dataframes_difference(dirty_df, clean_df)
    tp = 0
    fp = 0
    all_errs = len(diff_dict)
    context = gx.get_context()
    clean_validator = context.sources.pandas_default.read_csv(CLEAN_FILE_PATH)

    missingness_assistant_result = context.assistants.missingness.run(validator=clean_validator)
    volume_assistant_result = context.assistants.volume.run(validator=clean_validator)

    expectation_suite = missingness_assistant_result.get_expectation_suite()
    for expectations in volume_assistant_result.get_expectation_suite().expectations:
        expectation_suite.append_expectation(expectations)

    dirty_validator = context.sources.pandas_default.read_csv(DIRTY_FILE_PATH)
    dirty_validator.expectation_suite = expectation_suite
    dirty_validator.save_expectation_suite(discard_failed_expectations=False)
    checkpoint = context.add_or_update_checkpoint(name="dirty_data_checkpoint", validator=dirty_validator)
    checkpoint_result = checkpoint.run()
    for result in checkpoint_result["run_results"][list(checkpoint_result["run_results"].keys())[0]]['validation_result']["results"]:
        if not result["success"]:
            if result["expectation_config"]["expectation_type"] == "expect_column_values_to_not_be_null":
                column = result["expectation_config"]["kwargs"]["column"]
                print(f"Column {column} has missing values")
                missing_values = dirty_df[column].apply(lambda x: x=='')
                for i in range(len(missing_values)):
                    if missing_values[i]:
                        if (i, dirty_df.columns.get_loc(column)) in diff_dict:
                            tp += 1
                        else:
                            fp += 1
                    
    return tp, fp, all_errs 

def main(dataset_path):
    tp_all = 0
    fp_all = 0
    all_errs_all = 0

    for dir in os.listdir(dataset_path):
        dirty_file_path = os.path.join(dataset_path, dir, "dirty.csv")
        clean_file_path = os.path.join(dataset_path, dir, "clean.csv")
        tp, fp, all_errs = process_table(dirty_file_path, clean_file_path)
        tp_all += tp
        fp_all += fp
        all_errs_all += all_errs
    precision = tp_all / (tp_all + fp_all)
    recall = tp_all / all_errs_all
    f1 = 2 * (precision * recall) / (precision + recall)
    print("all_errs: ", all_errs_all)
    print(f"Precision: {precision}")
    print(f"Recall: {recall}")
    print(f"F1: {f1}")


if __name__ == '__main__':
    main("/Users/fatemehahmadi/Documents/matelda-deequ/rein_fixed")