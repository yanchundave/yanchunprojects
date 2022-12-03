import pandas as pd
import matplotlib.pyplot as plt

path = "/Users/yanchunyang/Documents/datafiles/Rfile/advance/"
margin_file = "margin_total.csv"
R_metrics_file = "R_metrics_advance_1.csv"

margin = pd.read_csv(path + margin_file, header=0)
margin = margin.drop(['Unnamed: 0'], axis=1)
metrics = pd.read_csv(path + R_metrics_file, header=0)

column_name = list(metrics['rn'])
column_color = ["green", "blue", "orange", "violet", "red", "tan", "darkcyan", "skyblue", "green", "violet", 'deeppink', 'lime']

color_map = {column_name[i]: column_color[i] for i in range(0, 12)}

def draw_response(column_name):
    for item in column_name:
        print(item)
        cost_list = [0]
        response_list = [0]
        cost = metrics.loc[metrics['rn']==item, ['mean_spend']].values[0][0]
        first_response = metrics.loc[metrics['rn']==item, ['mean_response']].values[0][0]

        cost_list.append(cost)
        response_list.append(first_response)

        other_response = list(margin[item])
        for i, subitem in enumerate(other_response):
            t = (i+1) * 0.05 * cost
            cost_list.append(cost + t)
            response_list.append(first_response + t * subitem)
        plt.scatter(cost_list[1], response_list[1], color=color_map[item])
        plt.plot(cost_list, response_list, color=color_map[item], label=item)
        plt.title("Increase the spending to 150% ")
        plt.legend()

def main():
    first_channels = ['Adwords_iOS', 'Apple_Search_Ads_iOS','Facebook_Android']
    second_channels = ['bytedanceglobal_int_Android','bytedanceglobal_int_iOS','Snapchat_iOS']
    third_channels = [ 'Tatari_TV','Facebook_iOS','Adwords_Android','new_channels']

    draw_response(first_channels)
    draw_response(second_channels)
    draw_response(third_channels)

if __name__ == '__main__':
    main