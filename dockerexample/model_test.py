import pickle
import numpy as np

data = np.array([79545.45857, 5.682861322, 7.009188143, 4.09, 23086.8005]).reshape(1, -1)
with open('model.p', 'rb') as f:
    model =pickle.load(f)
print(model.predict(data))
print("result is shown")