from tensorflow.keras import layers
from tensorflow.keras.models import Model

def create_model(cnn_input, cnn_output, ann_input, ann_out, final_output):

    cnn_model = create_cnn(cnn_input, cnn_output)
    ann_model = create_ann(ann_input, ann_out)

    combined_input = layers.concatenate([cnn_model.output, ann_model.output])
    combined = layers.Dense(16, activation='relu')(combined_input)
    combined = layers.Dense(final_output, activation='softmax')(combined)

    model = Model(inputs=[cnn_model.input, ann_model.input], outputs=combined)
    return model


def create_cnn(cnn_input, cnn_output):
    input_values = layers.Input(shape=cnn_input)
    x = layers.Conv2D(300, (3, 3), activation='relu', padding="same")(input_values)
    x = layers.MaxPooling2D((2,2))(x)
    x = layers.Conv2D(128, (3,3), activation='relu', padding='same')(x)
    x = layers.MaxPooling2D((2,2))(x)
    x = layers.Flatten()(x)
    x = layers.Dense(64, activation='relu')(x)
    x = layers.Dropout(0.5)(x)
    x = layers.Dense(7, activation='softmax')(x)
    x = Model(inputs=input_values, outputs = x)
    return x 


def create_ann(ann_input, ann_output):
    input_values = layers.Input(shape=ann_input)
    y = layers.Dense(64, activation='relu')(input_values)
    y = layers.Dense(32, activation='relu')(y)
    y = layers.Dropout(0.2)(y)
    y = layers.Dense(7, activation='relu')(y)
    y = Model(inputs=input_values, outputs = y)
    return y 


    
    