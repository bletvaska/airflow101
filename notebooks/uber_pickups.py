import datetime
from time import sleep

import pandas as pd
import streamlit as st


@st.cache_data
def load_data():
    df = pd.read_csv(
        "uber.csv",
        # index_col=0,
        header=0,
        names=[
            "key",
            "dt",
            "price",
            "pickup",
            "pickup_lon",
            "pickup_lat",
            "dropoff_lon",
            "dropoff_lat",
            "passengers",
        ],
        dtype={
            "passengers": "uint16",
            "price": "float32",
            "pickup_lon": "float32",
            "pickup_lat": "float32",
            "dropoff_lon": "float32",
            "dropoff_lat": "float32",
            "key": "uint32",
        },
        # parse_dates=['dt', 'pickup'],
        # date_format='%Y-%m-%d %H:%M:%S %Z',
    )

    # normalizing datetime columns
    df["pickup"] = pd.to_datetime(
        df["pickup"],
        format="%Y-%m-%d %H:%M:%S UTC",
        utc=True,
    ).dt.as_unit("s")

    df["dt"] = pd.to_datetime(
        df["dt"],
        format="ISO8601",
        utc=True,
    ).dt.as_unit("s")

    sleep(9)

    return df


# page configuration
st.set_page_config(
    page_title="Uber Taxi",
    layout="centered",  # centered | wide
    page_icon="🚕",
)


# st.title("Uber pickups in NYC")
# st.image('https://www.investopedia.com/thmb/rRFyB5UPC1a0_X6AUSw8owO7eso=/1500x0/filters:no_upscale():max_bytes(150000):strip_icc()/GettyImages-1923331422-852a4015c4534c60acd38b48dcd2c392.jpg')
# st.text('Pozrieme sa bližšie na jednotlivé jazdy taxíkmi spoločnosťou Uber v meste New York.')


"""
# Uber pickups in NYC

![Uber Car](https://www.investopedia.com/thmb/rRFyB5UPC1a0_X6AUSw8owO7eso=/1500x0/filters:no_upscale():max_bytes(150000):strip_icc()/GettyImages-1923331422-852a4015c4534c60acd38b48dcd2c392.jpg)

Pozrieme sa bližšie na jednotlivé jazdy taxíkmi spoločnosťou **Uber** v meste **New York**.
"""

# "### Nahrávanie dát"
st.subheader("Nahrávanie dát")

data_load_state = st.text("Nahrávam dataset")
df = load_data()
data_load_state.text("Nahrávam dataset... Nahraté.")

# show/hide dataframe in app
if st.checkbox("Zobraziť dáta") is True:
    st.write(df)


"## Prehľad jázd za jeden deň"

# date picker
date = st.date_input(
    label="**Vyber dátum**",
    value=datetime.date(2014, 1, 1),
    min_value=df["pickup"].min().date(),
    max_value=df["pickup"].max().date(),
)

# Filter dataframe to only include rows with the selected date
filtered_df = df.loc[df["pickup"].dt.date == date, :]
# Group by hour and sum prices for each hour
data = filtered_df.groupby(filtered_df["pickup"].dt.hour)["price"].sum()

# Display bar chart with hourly price sums
st.bar_chart(data, x_label="cas")

profit = filtered_df['price'].sum()
f'Zisk: **${profit:.2f}**'


f'## Nástupy na mape zo dňa {date}'

hour = st.slider('**Vyber hodinu:**', 0, 23, 12 )

st.map(
    data = None,
    latitude = None, 
    longitude = None, 
    zoom = 4
)


"well done"
