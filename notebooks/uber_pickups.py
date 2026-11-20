import pandas as pd
import streamlit as st


def load_data():
    df = pd.read_csv('uber.csv', 
        # index_col=0, 
        header=0,
        names=['key', 'dt', 'price', 'pickup', 'pickup_lon', 'pickup_lat', 'dropoff_lon', 'dropoff_lat', 'passengers'],
        dtype={
                "passengers": "uint16",
                "price": "float16",
                "pickup_lon": "float16",
                "pickup_lat": "float16",
                "dropoff_lon": "float32",
                "dropoff_lat": "float32",
                "key": "uint32",
        },
        #parse_dates=['dt', 'pickup'],
        #date_format='%Y-%m-%d %H:%M:%S %Z',
    )

    # normalizing datetime columns
    df['pickup'] = pd.to_datetime( 
        df["pickup"], 
        format="%Y-%m-%d %H:%M:%S UTC", 
        utc=True, 
    ).dt.as_unit("s")

    df['dt'] = pd.to_datetime( 
        df["dt"], 
        format="ISO8601", 
        utc=True, 
    ).dt.as_unit("s")

    return df
    

# st.title("Uber pickups in NYC")
# st.image('https://www.investopedia.com/thmb/rRFyB5UPC1a0_X6AUSw8owO7eso=/1500x0/filters:no_upscale():max_bytes(150000):strip_icc()/GettyImages-1923331422-852a4015c4534c60acd38b48dcd2c392.jpg')
# st.text('Pozrieme sa bližšie na jednotlivé jazdy taxíkmi spoločnosťou Uber v meste New York.')



"""
# Uber pickups in NYC

![Uber Car](https://www.investopedia.com/thmb/rRFyB5UPC1a0_X6AUSw8owO7eso=/1500x0/filters:no_upscale():max_bytes(150000):strip_icc()/GettyImages-1923331422-852a4015c4534c60acd38b48dcd2c392.jpg)

Pozrieme sa bližšie na jednotlivé jazdy taxíkmi spoločnosťou **Uber** v meste **New York**.
"""
