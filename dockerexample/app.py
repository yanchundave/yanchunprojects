import pickle
import numpy as np
from flask import Flask, request

model = None
app = Flask(__name__)

def load_model():
    global model
    with open('model.p', 'rb') as f:
        model = pickle.load(f)

@app.route('/')
def home_endpoint():
    return 'Hello World'

@app.route('/predict', methods=['POST'])
def get_prediction():
    if request.method == 'POST':
        data = request.get_json()
        t_data = list(data.values())
        s_data = np.array(t_data).reshape(1, -1)
        prediction = model.predict(s_data)
    return str(prediction[0])

if __name__ == '__main__':
    load_model()
    app.run(host='0.0.0.0', port=80)