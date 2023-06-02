import pandas as pd
import altair as alt

df = pd.read_csv('../dataset.csv', sep=';')
df['dt'] = pd.to_datetime(df['dt'], unit='s')

chart = alt.Chart(df[:20]).mark_area(
    ).encode(
    x='dt',
    y='temp'
)
chart



import altair as alt
from vega_datasets import data


source = data.stocks()

base = alt.Chart(source).encode(
    alt.Color("symbol").legend(None)
).transform_filter(
    "datum.symbol !== 'IBM'"
).properties(
    width=500
)

line = base.mark_line().encode(x="date", y="price")


last_price = base.mark_circle().encode(
    alt.X("last_date['date']:T"),
    alt.Y("last_date['price']:Q")
).transform_aggregate(
    last_date="argmax(date)",
    groupby=["symbol"]
)

company_name = last_price.mark_text(align="left", dx=4).encode(text="symbol")

chart = (line + last_price + company_name).encode(
    x=alt.X().title("date"),
    y=alt.Y().title("price")
)

chart





import pandas as pd
import pendulum
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import tempfile

df = pd.read_csv('../dataset.csv', sep=';')
df['dt'] = pd.to_datetime(df['dt'], unit='s')


filter_yesterday = (
    (df['dt'] >= pendulum.yesterday('utc').to_datetime_string()) 
    & (df['dt'] < pendulum.today('utc').to_datetime_string())
)

yesterday  = df.loc[filter_yesterday]


_, ax = plt.subplots()
yesterday_date = pendulum.yesterday("utc").strftime("%d.%m.%Y")
ax.set_title(f"Teplota zo dňa {yesterday_date}")
ax.set_xlabel("čas (hod)")
ax.set_ylabel("teplota (°C)")
ax.plot(yesterday["dt"].array, yesterday["temp"].array)
ax.xaxis.set_major_formatter(mdates.DateFormatter("%H"))

_, tmp_path = tempfile.mkstemp(suffix=".png")
plt.savefig('graph.png') # tmp_path)




