import numpy as np 
import pandas as pd 
import matplotlib.pyplot as plt


datafile_path = "/Users/yanchunyang/Documents/datafiles/pystan/"
def compare_result(filename_1, filename_2):
    origin_df = pd.read_csv(datafile_path + filename_1, header=0)
    actual_df = pd.read_csv(datafile_path + filename_2, header=0)
    origin_sum = origin_df.apply(sum, axis=0)
    actual_sum = actual_df.apply(sum, axis=0)
    print(origin_sum)
    print(actual_sum)


def main():
    filename_1 = "onedave.csv"
    filename_2 = "actual_user.csv"
    compare_result(filename_1, filename_2)


if __name__ == '__main__':
    main()

