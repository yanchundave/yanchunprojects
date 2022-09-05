1. event2vec model to data engineering  7/5 - 7/8
2. Extract all the users' last event timestamp  7/11
3. Extract their last event         7/11
4. Convert their events to vector   7/12
5. Collect other features: 7/12 - 7/13
    1. Transaction amount
    2. Frequency
    3. Last Event timestamp
    4. Actions
    5. Events vectors
6. Set up a XGBoost model 
7. Set up a NN model 
8. Verify the result
9. Update the model 

Above model doesn't work

New idea

1. Use Conv2D for revenue (daily)
2. Use Conv2D for activity (daily)
3. If possible, activity could be drawn as 3D(daily)
4. Revenue to draw different category
5. Convert Revenue prediction to image prediction 