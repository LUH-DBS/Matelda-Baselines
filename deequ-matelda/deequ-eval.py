import os
import html 
import re
import warnings 
import pandas as pd 
import os
import sys 
import pickle 

warnings.filterwarnings("ignore", message="p-value may not be accurate for N > 5000")

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


deequ_res = "/Users/fatemehahmadi/IdeaProjects/deequ-matelda/datasets/REIN_matelda_idx"
orig = "/Users/fatemehahmadi/Documents/matelda-deequ/rein_fixed"
tp = 0
fp = 0
all_errs = 0
for dir in os.listdir(deequ_res):
    dirty_df = read_csv_dataset(os.path.join(orig, dir, "dirty.csv"))
    clean_df = read_csv_dataset(os.path.join(orig, dir, "clean.csv"))
    diff_dict = get_dataframes_difference(dirty_df, clean_df)
    all_errs += len(diff_dict)
    if os.path.exists(os.path.join(deequ_res, dir, "result_clean")):
        for file in os.listdir(os.path.join(deequ_res, dir, "result_clean")):
            if file.endswith(".csv"):
                res_df = read_csv_dataset(os.path.join(deequ_res, dir, "result_clean", file))
                
                for i, row in res_df.iterrows():
                    if row[1] not in dirty_df.columns:
                        print("Column not found: ", row[1])
                        continue
                    if (int(row[0]), dirty_df.columns.get_loc(row[1])) in diff_dict:
                        tp += 1
                    else:
                        fp += 1
                

precision = tp / (tp + fp)
recall = tp / all_errs
f1 = 2 * (precision * recall) / (precision + recall)

print("all_errs: ", all_errs)
print(f"Precision: {precision}")
print(f"Recall: {recall}")
print(f"F1: {f1}")