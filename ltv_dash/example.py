from dash import Dash, html, dcc, Input, Output
import plotly.express as px
import pandas as pd
import plotly.graph_objects as go

app = Dash(__name__)

"""This is the data input for presentation"""
df = pd.DataFrame({
    "date": ["2022-01-01", "2022-02-01", "2022-03-01", "2022-04-01","2022-05-01", "2022-06-01","2022-07-01", "2022-08-01",],
    "retention_rate": [0.5,0.51,0.35,0.56,0.58,0.57,0.48,0.69],
    "advance_taker_pcnt": [0.5,0.51,0.35,0.56,0.58,0.57,0.48,0.69],
    "avg_advance_number_per_taker":[0.5,0.51,0.35,0.56,0.58,0.57,0.48,0.69],
    "avg_principle_size":[100, 110, 102, 103, 114, 105, 106, 107],
    "avg_tip_pcnt_of_principle":[0.5,0.51,0.35,0.56,0.58,0.57,0.48,0.69],
    "avg_fee_pcnt_of_principle":[0.5,0.51,0.35,0.56,0.58,0.57,0.48,0.69]
})

df_predict = pd.DataFrame({
    "date": ["2022-09-01", "2022-10-01", "2022-11-01","2022-12-01"],
    "retention_rate": [0.6,0.7,0.75,0.86],
    "advance_taker_pcnt": [0.5,0.51,0.35,0.56],
    "avg_advance_number_per_taker":[0.5,0.51,0.35,0.56],
    "avg_principle_size":[100, 106, 105, 108],
    "avg_tip_pcnt_of_principle":[0.5,0.51,0.35,0.56],
    "avg_fee_pcnt_of_principle":[0.5,0.51,0.35,0.56]
})
df['prediction'] = df['retention_rate'] * df["advance_taker_pcnt"] * df["avg_advance_number_per_taker"] * df["avg_principle_size"] * \
df["avg_tip_pcnt_of_principle"] * df["avg_fee_pcnt_of_principle"]

df_predict['prediction'] = df_predict['retention_rate'] * df_predict["advance_taker_pcnt"] * df_predict["avg_advance_number_per_taker"] * df_predict["avg_principle_size"] * \
df_predict["avg_tip_pcnt_of_principle"] * df_predict["avg_fee_pcnt_of_principle"]

"""
This part is to construct the basic graphs
"""
fig_1 = go.Figure([
        go.Line(name="real", x=df['date'], y=df['retention_rate']),
        go.Scatter(name="predict", x=df_predict['date'], y=df_predict['retention_rate'])
    ])
fig_2 = go.Figure([
        go.Line(name="real", x=df['date'], y=df['advance_taker_pcnt']),
        go.Scatter(name="predict", x=df_predict['date'], y=df_predict['advance_taker_pcnt'])
    ])
fig_3 = go.Figure([
        go.Line(name="real", x=df['date'], y=df['avg_advance_number_per_taker']),
        go.Scatter(name="predict", x=df_predict['date'], y=df_predict['avg_advance_number_per_taker'])
    ])
fig_4 = go.Figure([
        go.Line(name="real", x=df['date'], y=df['avg_principle_size']),
        go.Scatter(name="predict", x=df_predict['date'], y=df_predict['avg_principle_size'])
    ])
fig_5 = go.Figure([
        go.Line(name="real", x=df['date'], y=df['avg_tip_pcnt_of_principle']),
        go.Scatter(name="predict", x=df_predict['date'], y=df_predict['avg_tip_pcnt_of_principle'])
    ])
fig_6 = go.Figure([
        go.Line(name="real", x=df['date'], y=df['avg_fee_pcnt_of_principle']),
        go.Scatter(name="predict", x=df_predict['date'], y=df_predict['avg_fee_pcnt_of_principle'])
    ])
fig_7 = go.Figure([
        go.Line(name="real", x=df['date'], y=df['prediction']),
        go.Scatter(name="predict", x=df_predict['date'], y=df_predict['prediction'])
    ])

""""
This part is to insert these graphs into html
"""
app.layout = html.Div(children=[
    html.H1(children='Revenue Prediction'),
    html.Div(children=[

        html.Div( children=[
             dcc.Graph(id='r1',figure=fig_1),
             dcc.Slider(1, 1.2, 0.05, value=1, id='r1_percentage')
        ], style={'width':'33%','display': 'inline-block'}),

        html.Div( children=[
             dcc.Graph(id='r2',figure=fig_2),
             dcc.Slider(1, 1.2, 0.05, value=1, id='r2_percentage')
        ], style={'width':'33%','display': 'inline-block'}),

        html.Div( children=[
             dcc.Graph(id='r3',figure=fig_3),
             dcc.Slider(1, 1.2, 0.05, value=1, id='r3_percentage')
        ], style={'width':'33%','display': 'inline-block'})
    ], style={'height':'30%'}),

    html.Div(children=[

        html.Div( children=[
             dcc.Graph(id='r4',figure=fig_4),
             dcc.Slider(1, 1.2, 0.05, value=1, id='r4_percentage')
        ], style={'width':'33%','display': 'inline-block'}),

        html.Div( children=[
             dcc.Graph(id='r5',figure=fig_5),
             dcc.Slider(1, 1.2, 0.05, value=1, id='r5_percentage')
        ], style={'width':'33%','display': 'inline-block'}),

        html.Div( children=[
             dcc.Graph(id='r6',figure=fig_6),
             dcc.Slider(1, 1.2, 0.05, value=1, id='r6_percentage')
        ], style={'width':'33%','display': 'inline-block'})
    ], style={'height':'30%'}),

    html.Div(children=[
        dcc.Graph(id='r7',figure=fig_7)
    ], style={'height':'30%'})

])

"""
This part is to make the graph dynamic. Input is to adjust the slider. Output is to replace the graph based on slider
"""
@app.callback(
    Output('r1', 'figure'),
    Input('r1_percentage', 'value')
)
def update_figure1(selected_percentage):
    print(selected_percentage)
    df_predict['retention_update'] = df_predict['retention_rate'] * float(selected_percentage)
    #df['predict_update'] = df['predict'] * float(selected_percentage)
    fig_1_update = go.Figure([
        go.Line(name="predict_update", x=df_predict['date'], y=df_predict['retention_update'], line_dash='dot'),
        go.Line(name="predict_origin", x=df_predict['date'], y=df_predict['retention_rate']),
        go.Scatter(name="origin", x=df['date'], y=df['retention_rate'])
    ])
    #fig_2_update = px.line(df, x="date", y="predict_update")
    fig_1_update.update_layout(transition_duration=500)
    return fig_1_update

"""
This part is to adjust the second graph based on the first slider
"""
@app.callback(
    Output('r2', 'figure'),
    Input('r1_percentage', 'value')
)
def update_figure2(selected_percentage):
    print(selected_percentage)
    df_predict['advance_taker_pcnt_update'] = df_predict['advance_taker_pcnt'] * (0.5 + float(selected_percentage)/2)
    #df['predict_update'] = df['predict'] * float(selected_percentage)

    fig_2_update = go.Figure([
        go.Line(name="predict_u", x=df_predict['date'], y=df_predict['advance_taker_pcnt_update'], line_dash='dot'),
        go.Line(name="predict_origin", x=df_predict['date'], y=df_predict['advance_taker_pcnt']),
        go.Scatter(name="origin", x=df['date'], y=df['advance_taker_pcnt'])
    ])
    #fig_2_update = px.line(df, x="date", y="predict_update")
    fig_2_update.update_layout(transition_duration=500)
    return fig_2_update

"""
This part is to adjust the final solution based on the first slider. Based on the same pattern, you can add more interactive functions
"""
@app.callback(
    Output('r7', 'figure'),
    Input('r1_percentage', 'value')
)
def update_figure2(selected_percentage):
    df_predict['prediction_update'] = df_predict['prediction'] * (float(selected_percentage))
    #df['predict_update'] = df['predict'] * float(selected_percentage)

    fig_7_update = go.Figure([
        go.Line(name="predict_u", x=df_predict['date'], y=df_predict['prediction_update'], line_dash='dot'),
        go.Line(name="predict_origin", x=df_predict['date'], y=df_predict['prediction']),
        go.Scatter(name="origin", x=df['date'], y=df['prediction'])
    ])
    #fig_2_update = px.line(df, x="date", y="predict_update")
    fig_7_update.update_layout(transition_duration=500)
    return fig_7_update

if __name__ == '__main__':
    app.run_server(debug=True)