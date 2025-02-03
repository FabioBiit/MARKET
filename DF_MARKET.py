import pandas as pd
import datetime

df = pd.read_csv("C:/Users/kyros/OneDrive/Desktop/MARKET/risultati_arbitraggio_new_market_positivo.csv", sep=",", encoding="utf-8", parse_dates=["Data_e_ora"])

df_query = df.where(df["Capitale"] == 100).dropna()
df_query["Guadagno_netto"] = df_query["Guadagno_netto"].str.split(" ").str[1].astype(float)

delta_time = max(df_query["Data_e_ora"]) - min(df_query["Data_e_ora"])

tot_guadagno = df_query["Guadagno_netto"].sum()

print(df_query)

print(f"\nTotale guadagno:{tot_guadagno:.2f} USDT in {delta_time}")

# query.info()