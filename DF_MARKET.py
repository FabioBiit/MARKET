import pandas as pd
import datetime

# df = pd.read_csv("C:/Users/kyros/OneDrive/Desktop/MARKET/risultati_arbitraggio_new_market_positivo.csv", sep=",", encoding="utf-8", parse_dates=["Data_e_ora"])

df = pd.read_csv("C:/Users/kyros/OneDrive/Desktop/MARKET/risultati_arbitraggio_new_market_positivo_v2.csv", sep=",", encoding="utf-8", parse_dates=["Data_e_ora"])

df["Guadagno_netto"] = df["Guadagno_netto"].str.split(" ").str[1].astype(float)
df["Moneta"] = df["Moneta"].str.split(" ").str[1].astype(str)

df["Exchange_acq"] = df["Exchange_acq"].str.strip()
df["Exchange_ven"] = df["Exchange_ven"].str.strip()

# df = df[( df["Moneta"].isin(["ADA", "XRP", "SOL"]) ) & ( df["Guadagno_netto"] >= 5.00 ) & ( df["Exchange_acq"] != 'Bitfinex' ) & ( df["Exchange_ven"] != 'Bitfinex')]

# df = df[( df["Moneta"].isin(["ADA", "XRP", "SOL"]) ) & ( df["Guadagno_netto"] >= 5.00 ) & ( df["Exchange_ven"] != 'Bitfinex')]

# df = df[( df["Moneta"].isin(["ADA", "XRP", "SOL"]) ) & ( df["Guadagno_netto"] >= 5.00 )]

# df = df[(df["Guadagno_netto"] >= 5.00)]

if not df.empty:
    delta_time = max(df["Data_e_ora"]) - min(df["Data_e_ora"])
    tot_guadagno = df["Guadagno_netto"].sum()
    print(df)
    print(f"\nTotale guadagno:{tot_guadagno:.2f} USDT in {delta_time}")
else:
    print(df)

# query.info()