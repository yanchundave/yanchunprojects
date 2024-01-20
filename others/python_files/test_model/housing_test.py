import pickle
import pandas as pd

def main():
    test = pickle.load(open( "/Users/yanchunyang/github/yanchunprojects/others/python_files/model.p", "rb" ))
    sample = pd.read_csv("sample.txt")
    print(sample)
    test_result = test.predict(sample.loc[:, ['income', 'age', 'rooms', 'bedrooms', 'population']])
    pd.Series(test_result).to_csv("sample_result.txt")
    print("test done")

if __name__ == '__main__':
    main()


