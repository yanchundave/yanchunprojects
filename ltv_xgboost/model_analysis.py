import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import  math
from sklearn.metrics import roc_curve, auc

# Draw a simple line

def draw_line():
    x = [str(int(math.pow(10, i))) for i in range(1, 5)]
    y = [mape_value[i][1]  for i in range(1, 5 )]
    fig, ax = plt.subplots()
    ax.plot(x, y)

    # Set the y-axis label to percentage
    ax.yaxis.set_major_formatter(ticker.FuncFormatter(lambda y, _: '{:.0%}'.format(y)))

    # Change the x-axis label
    ax.set_xlabel('cohort size')

    # Change the y-axis label
    ax.set_ylabel('Percentage')
    for i in range(len(x)):
        ax.annotate(f'{y[i]:.2%}', (x[i], y[i]), textcoords="offset points", xytext=(0,5), ha='center')

    # Set plot title
    ax.set_title('MAPE by Cohort Size')

    plt.show()

# draw two bars
def draw_twobars(df, x_col, y_col1, y_col2, title_name):
    bar_width = 0.15
    index = np.arange(df.shape[0])
    fig, ax = plt.subplots(figsize=(8, 6))
    bar1 = ax.bar(df[x_col], df[y_col1], bar_width, label=y_col1)
    bar2 = ax.bar(index + bar_width, df[y_col2], bar_width, label=y_col2)

    for bar in bar1:
        yval = bar.get_height()
        ax.annotate(f'{yval:.1f}', xy=(bar.get_x() + bar.get_width() / 2, yval),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')

    for bar in bar2:
        yval = bar.get_height()
        ax.annotate(f'{yval:.1f}', xy=(bar.get_x() + bar.get_width() / 2, yval),
                    xytext=(0, 3),  # 3 points vertical offset
                    textcoords="offset points",
                    ha='center', va='bottom')
    print(df[x_col])
    ax.set_xlabel(x_col)
    ax.set_ylabel('LTV')
    ax.set_title(title_name)
    #ax.set_xticks(index + bar_width / 2)
    ax.set_xticklabels(df[x_col])
    plt.xticks(rotation=45)
    ax.legend(loc='lower left', bbox_to_anchor=(0, 0))

    # Display the plot
    plt.show()

#slice dimensions
def dimension_slice(df, dimension):
    dimension_group = df.groupby([dimension]).agg(
    {'arpu_predict':'mean', 'churn_predict':'mean', 'ADVANCE_LIFETIME_REVENUE_TOTAL':'mean', 'USER_ID':'nunique'}).reset_index()
    dimension_group['LTV'] = dimension_group['arpu_predict']/dimension_group['churn_predict'] + dimension_group['ADVANCE_LIFETIME_REVENUE_TOTAL']
    dimension_group_update = dimension_group.sort_values(by=['USER_ID'], ascending=False)
    draw_twobars(dimension_group_update.iloc[0:10], dimension, 'LTV', 'ADVANCE_LIFETIME_REVENUE_TOTAL', "LTV by " + dimension)

#draw line and bar combination
def draw_combine(df, x_col, bar_col,line_col, title):
    fig, ax1 = plt.subplots(figsize=(8, 6))

    # Bar chart for column1
    ax1.bar(df[x_col], df[bar_col], color='g', label=bar_col)
    ax1.set_xlabel('User Tenure by Month')
    ax1.set_ylabel('Unique User Counts', color='g')
    ax1.tick_params(axis='y', labelcolor='g')

    # Create a second y-axis for the line chart
    ax2 = ax1.twinx()
    ax2.plot(df[x_col], df[line_col], color='b', marker='o', label=line_col)
    ax2.set_ylabel(line_col, color='b')
    ax2.tick_params(axis='y', labelcolor='b')

    # Add title and show plot
    plt.title(title)
    fig.tight_layout()
    plt.show()


#draw two line comparison
def draw_comparison(df, x_col, line_1,line_2, y_label, title, label1, label2):
    fig, ax1 = plt.subplots(figsize=(8, 6))

    # Bar chart for column1
    ax1.plot(df[x_col], df[line_1], color='g', marker='x', label=label1)
    ax1.set_xlabel('User Tenure by Month')
    ax1.set_ylabel(y_label, color='g')
    ax1.tick_params(axis='y', labelcolor='g')
    ax1.plot(df[x_col], df[line_2], color='b', marker='o', label=label2)
    # Add title and show plot
    plt.title(title)
    plt.legend()
    fig.tight_layout()
    plt.show()


# draw auc line
def draw_roc(y_true, y_predict):
    fpr, tpr, thresholds = roc_curve(y_true, y_predict)
    roc_auc = auc(fpr, tpr)
    plt.figure()
    plt.plot(fpr, tpr, color='darkorange', lw=2, label=f'ROC curve (area = {roc_auc:.2f})')
    plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
    plt.xlim([0.0, 1.0])
    plt.ylim([0.0, 1.05])
    plt.xlabel('False Positive Rate')
    plt.ylabel('True Positive Rate')
    plt.title('Receiver Operating Characteristic')
    plt.legend(loc="lower right")
    plt.show()

