from sklearn.preprocessing import LabelEncoder
import streamlit as st
import pandas as pd
import pickle
import numpy as np

# Function to encode categorical columns in the DataFrame
def encode_dataframe(df):
    label_encoder = LabelEncoder()
    columns_to_encode = ['job', 'merchant', 'street', 'location']
    
    for column in columns_to_encode:
        df[column] = label_encoder.fit_transform(df[column])
    df['gender'] = df['gender'].replace({'M': 0, 'F': 1})
    
    return df

# Function to load the pickled model
def load_model(model_file):
    with open(model_file, 'rb') as file:
        model = pickle.load(file)
    return model

# Function to make predictions using the loaded model
def predict(model, input_data):
    predictions = model.predict(input_data)
    return predictions

def main(model_file):
    st.title("Credit Card Fraud Detection")
    st.sidebar.title("Enter Merchant Information")
    
    # Sidebar input fields
    cc_num = st.sidebar.number_input("Credit Card Number")
    merchant_name = st.sidebar.text_input("Merchant Name")
    amt = st.sidebar.number_input("Amount", step=0.1)
    gender = st.sidebar.radio("Gender", ('M', 'F'))
    street = st.sidebar.text_input("Street")
    zip_code = st.sidebar.number_input("Zip Code")
    lat = st.sidebar.number_input("Latitude", format="%.4f")
    long = st.sidebar.number_input("Longitude", format="%.4f")
    city_pop = st.sidebar.number_input("Population")
    job = st.sidebar.text_input("Job")
    unix_time = st.sidebar.number_input("Unix Time")
    merch_lat = st.sidebar.number_input("Merchant Latitude", format="%.4f")
    merch_long = st.sidebar.number_input("Merchant Longitude", format="%.4f")
    location = st.sidebar.text_input("Location")

    # Check if all required fields are filled
    if cc_num and merchant_name and amt and gender and street and zip_code and lat and long  and city_pop and job and unix_time and merch_lat and merch_long and location:
        input_data = {"cc_num": cc_num, "merchant": merchant_name, "amt": amt, "gender": gender,
                      "street": street, "zip_code": zip_code, "lat": lat,
                      "long": long, "city_pop": city_pop, "job": job, "unix_time": unix_time, "merch_lat": merch_lat,
                      "merch_long": merch_long, "location": location}
    else:
        st.warning("Please fill in all fields.")
        return

    # Convert input data to DataFrame
    df_new_data = pd.DataFrame(input_data, index=[0])

    # Encode categorical columns
    final_result = encode_dataframe(df_new_data)

    # Load the model
    model = load_model(model_file)
    # Make predictions
    prediction_model = model.predict(final_result)
    probabilities = model.predict_proba(final_result)
    flat_probabilities = np.ndarray.flatten(probabilities)
  
    # Display prediction result
    if prediction_model[0] == 0:
        st.success(f"The Merchant is Non-Fradulent with accuracy: {np.around(flat_probabilities[0], 2)*100}%")
    else:
        st.error(f"The Merchant is Fradulent with accuracy: {np.around(flat_probabilities[1], 2)*100}%")
    
if __name__ == "__main__":
    model_file = 'fraud_detection_model.pkl'
    main(model_file)
