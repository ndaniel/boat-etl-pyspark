# Databricks notebook or script version of the boat ETL pipeline

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count
import pandas as pd
import numpy as np
from datetime import datetime
import io

# Function to convert price to euros
def euro(price, currency):
    try:
        price = float(price)
    except (ValueError, TypeError):
        return None
    rates = {"EUR": 1.0, "CHF": 1.06, "DKK": 0.13, "GBP": 1.17}
    return price * rates.get(currency, None)

# Clean Unicode characters
def only_ascii(text, special=False):
    if special:
        text = text.replace("£", "GBP")
        text = text.replace('Â»', '|')
    x = ''.join(c if ord(c) < 128 else " " for c in text)
    while True:
        x = x.replace("  ", " ")
        x = x.replace(" ,", ",")
        x = x.replace(' "','"')
        x = x.replace('" ', '"')
        if x.find("  ") == -1:
            break
    return x.strip()



replace_country = {
    # Valid countries (normalized)
    "switzerland": "Switzerland",
    "germany": "Germany",
    "denmark": "Denmark",
    "italy": "Italy",
    "france": "France",
    "united kingdom": "United Kingdom",
    "spain": "Spain",
    "austria": "Austria",
    "netherlands": "Netherlands",
    "slovenia": "Slovenia",
    "serbia": "Serbia",
    "slovakia": "Slovakia",
    "croatia": "Croatia",
    "portugal": "Portugal",
    "malta": "Malta",
    "montenegro": "Montenegro",
    "latvia": "Latvia",
    "greece": "Greece",
    "poland": "Poland",
    "turkey": "Turkey",
    "finland": "Finland",
    "hungary": "Hungary",
    "cyprus": "Cyprus",
    "czech republic": "Czech Republic",
    "sweden": "Sweden",
    "lithuania": "Lithuania",
    "united states": "United States",
    "ukraine": "Ukraine",
    "estonia": "Estonia",
    "monaco": "Monaco",
    "russia": "Russia",
    "egypt": "Egypt",
    "united arab emirates": "United Arab Emirates",
    "australia": "Australia",
    "bulgaria": "Bulgaria",
    "philippines": "Philippines",
    "taiwan": "Taiwan",
    "thailand": "Thailand",
    "luxembourg": "Luxembourg",
    "venezuela": "Venezuela",
    "ireland": "Ireland",
    "norway": "Norway",
    "seychelles": "Seychelles",
    "morocco": "Morocco",
    "lebanon": "Lebanon",
    "romania": "Romania",

    # Localized or typo versions of countries
    "italien": "Italy",
    "italie": "Italy",
    "dalmatien": "Croatia",
    "kroatien krk": "Croatia",
    "espa?a": "Spain",

    # Cities/regions mapped to countries
    "steinwiesen": "Germany",
    "rolle": "Switzerland",
    "baden baden": "Germany",
    "lake constance": "Germany",
    "split": "Croatia",
    "lago maggiore": "Italy",
    "brandenburg an derhavel": "Germany",
    "zevenbergen": "Netherlands",
    "faoug": "Switzerland",
    "martinique": "France",
    "gibraltar": "United Kingdom",
    "mallorca": "Spain",
    "opwijk": "Belgium",
    "isle of man": "United Kingdom",
    "neusiedl am see": "Austria",
    "bodensee": "Germany",
    "avenches": "Switzerland",
    "heilbronn": "Germany",
    "z richse, 8855 wangen sz": "Switzerland",
    "ibiza": "Spain",
    "lommel": "Belgium",
    "wijdenes": "Netherlands",
    "bremen": "Germany",
    "bielefeld": "Germany",
    "porto rotondo": "Italy",
    "berlin wannsee": "Germany",
    "toscana": "Italy",
    "vierwaldst ttersee - buochs": "Switzerland",
    "juelsminde havn": "Denmark",
    "barssel": "Germany",
    "welschenrohr": "Switzerland",
    "thun": "Switzerland",
    "adria": "Italy",
    "rovinij": "Croatia",                            # city in Croatia
    "donau": "Germany",                              # Danube river (German name)
    "travem nde": "Germany",                         # typo for Travemünde, Germany
    "stralsund": "Germany",                          # city in Germany
    "rostock": "Germany",                            # city in Germany
    "lake geneva": "Switzerland",                    # lake on Swiss–French border
    "belgi, zulte": "Belgium",                       # town in Belgium
    "niederrhein": "Germany",                        # region in Germany
    "r gen": "Germany",                              # typo for Rügen island
    "oder": "Germany",                               # river
    "beilngries": "Germany",                         # town in Bavaria
    "marina punat": "Croatia",                       # marina on Krk island
    "french southern territories": "France",         # overseas territory
    "brandenburg": "Germany",                        # state in Germany
    "nan": "None",                                # NaN entry
    "waren m ritz": "Germany",                       # Waren (Müritz), town in Germany
    "jersey": "United Kingdom",                      # British crown dependency
    "neustadt in holstein (ostsee)": "Germany",      # town in northern Germany
    "ostsee": "Germany",                             # Baltic Sea (German name)
    "greetsile/ krummh rn": "Germany",               # Greetsiel / Krummhörn, Lower Saxony
    "annecy": "France",                              # city in France
    "izola": "Slovenia",                             # coastal town
    "83278 traunstein": "Germany",                   # German town (postal code)
    "novi vinodolski": "Croatia",                    # coastal town
    "lago di garda": "Italy",                        # lake in northern Italy
    "nordseek ste": "Germany",                       # typo for Nordseeküste (North Sea coast)
    "24782 b delsdorf": "Germany",                   # Büdelsdorf, Germany
    "pt stkysten ellers esbjerg": "Denmark",         # likely Esbjerg (coastal Denmark)
    "calanova mallorca": "Spain",                    # marina in Mallorca
    "katwijk": "Netherlands",                        # coastal town
    "tenero, lago maggiore": "Switzerland",          # lakeside town
    "fu ach": "Austria",                             # typo for Fußach, Austria
    "angera": "Italy",                               # lakeside town
    "lago maggiore, minusio": "Switzerland",         # lakeside town
    "thalwil": "Switzerland",                        # suburb of Zürich
    "rheinfelden": "Germany"                         # could also be Switzerland, but likely German side
}

# Load and clean CSV (DBFS path assumed)
def load_and_clean_csv(path):
    dbutils.fs.cp(path, "file:/tmp/boat.csv", recurse=True)
    with open("/tmp/boat.csv", "r", encoding="utf-8") as f:
        lines = [line.rstrip("\r\n") for line in f if line.strip()]
        cleaned = [only_ascii(line, special=True) for line in lines]
    with open("/tmp/boat_cleaned.csv", "w", encoding="latin1") as f:
        f.writelines([line + "\n" for line in cleaned])
    df = pd.read_csv("/tmp/boat_cleaned.csv", encoding="latin1")
    return df

# Transform and validate
def transform(df):
    x = df["Price"].str.partition(" ")
    df["Currency"] = x[0]
    df["Price"] = x[2].astype(int)
    df["Euro"] = df.apply(lambda row: euro(row["Price"], row["Currency"]), axis=1)

    current_year = datetime.now().year
    year_col = "Year Built"
    m = min([y for y in df[year_col] if y and y != 0])
    m2 = m - 10
    df[year_col] = df[year_col].apply(lambda y: y if m <= y <= current_year else m2)

    x = df['Location'].str.split("|", n=1, expand=True)
    df['Country'] = x[0].str.strip().str.lower()
    df['City'] = x[1].str.strip()
    df.drop(columns=["Location"], inplace=True)

    # Optionally use REPLACE_COUNTRY here
    # df["Country"] = df["Country"].replace(REPLACE_COUNTRY)

    df.fillna({
        "Length": 0,
        "Width": 0,
        "Type": "None",
        "Manufacturer": "None",
        "Material": "None",
        "City": "None",
        "Country": "None"
    }, inplace=True)

    x = df["Type"].tolist()
    x = [e.partition(",") for e in x]
    power = [e[2] if e[2] else 'None' for e in x]
    df["Type"] = [e[0] for e in x]
    df["Power"] = power

    return df

# Main ETL function
def run_etl(input_path, output_path, summary_path):
    df = load_and_clean_csv(input_path)
    df = transform(df)

    spark = SparkSession.builder.appName("BoatETL").getOrCreate()

    # Rename columns for Spark
    df.rename(columns={"Euro": "price_eur", "Country": "country"}, inplace=True)
    sdf = spark.createDataFrame(df)

    # Save to Parquet
    sdf.write.mode("overwrite").parquet(output_path)

    # Create and save summary
    summary_df = (
        sdf.groupBy("country")
        .agg(avg("price_eur").alias("avg_price"), count("*").alias("count"))
        .orderBy("avg_price", ascending=False)
    )
    summary_df.toPandas().to_csv("/dbfs/" + summary_path, index=False)
    display(summary_df)

# Example usage — set paths here
run_etl(
    input_path="dbfs:/FileStore/data/boat_data.csv",
    output_path="dbfs:/FileStore/output/boats_parquet",
    summary_path="FileStore/output/boats_summary.csv"
)
