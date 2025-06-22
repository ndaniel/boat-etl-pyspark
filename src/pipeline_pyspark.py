#!/usr/bin/env python
# coding: utf-8
"""
ETL pipeline for cleaning, processing and loading the boat sales data.
To be run on Azure DataBricks.

Example:

 python pipeline_pyspark.py -i data/boats.csv -o output/boats.parquet -s output/summary.csv

 
 """

import sys
import os
import numpy as np
import pandas as pd
import pandera.pandas as pa
from datetime import datetime
import tempfile
import argparse
import logging

# Logging :-)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


# convert prices to Euros
def euro(price, currency):
    try:
        price = float(price)
    except (ValueError, TypeError):
        return np.nan
    rates = {"EUR": 1.0, "CHF": 1.06, "DKK": 0.13, "GBP": 1.17}
    return price * rates.get(currency, np.nan)


# some custom handling of unicodes
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
    x = x.strip()
    return x


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


###
def preprocess_csv(input_path):
    """
    Pre-processing of CSV is necessary. CSV file has as separator comma.
    """
    logger.info("Starting data transformation and validation...")

    with open(input_path, "r", encoding="utf-8") as f:
        lines = [line.rstrip("\r\n") for line in f if line.rstrip("\r\n")]
        cleaned = [only_ascii(e, special=True) for e in lines]

    with tempfile.NamedTemporaryFile(mode='w+', encoding="latin1", delete=False) as tmp:
        tmp.writelines([e + "\n" for e in cleaned])
        tmp.seek(0)
        df = pd.read_csv(tmp.name, encoding="latin1")

    return df
    
    
###
def transform_and_validate(d):
    """
    Transforming and validating the boat sales
    """
    logger.info("Starting data transformation and validation...")

    x = d["Price"].str.partition(" ")
    d["Currency"] = x[0]
    d["Price"] = x[2].astype(int)
    d["Euro"] = d.apply(lambda x: euro(x["Price"], x["Currency"]), axis=1)

    # see the histogram for the years
    YEAR_COL = "Year Built"

    # for the year built use the min year - 10 where is NaN 
    m = min([e for e in d[YEAR_COL] if e and e!=0])
    m2 = m - 10

    current_year = datetime.now().year
    d[YEAR_COL]=d[YEAR_COL].apply(lambda x: x if m <=x and x <= current_year else m2)


    #Split Location to Country and City
    x = d['Location'].str.split("|",n=1,expand=True)
    d['Country'] =x[0].str.rstrip()
    d['City'] = x[1].str.rstrip() 
    d.drop(columns=['Location'], inplace=True)

    d["Country"] = d["Country"].astype(str).str.strip().str.lower()
    d["Country"] = d["Country"].replace(replace_country)
    d['Country'] = d['Country'].replace("nan", np.nan).fillna('None')

    # Length - processing
    d['Length'] = d['Length'].fillna(0)
    
    # Width - processing
    d['Width'] = d['Width'].fillna(0)

    # Type - processing
    d['Type'] =d["Type"].fillna('None')

    # Type - processing
    x = d["Type"].tolist()
    x = [e.partition(",") for e in x]
    power = [e[2] if e[2] else 'None' for e in x]
    x = [e[0] for e in x]
    d["Type"]=x
    d["Power"]=power


    d['Manufacturer']=d['Manufacturer'].fillna('None')
    d["Material"].value_counts().sort_index()
    d['Material']=d['Material'].fillna('None')
    d['City']=d['City'].fillna('None')
    d['Country']=d['Country'].fillna('None')

    material = {'Aluminium',
     'Carbon Fiber',
     'GRP',
     'Hypalon',
     'None',
     'PVC',
     'Plastic',
     'Reinforced concrete',
     'Rubber',
     'Steel',
     'Thermoplastic',
     'Wood'}


    schema = pa.DataFrameSchema(
        {
            "Price": pa.Column(int),
            "Currency": pa.Column(str, pa.Check.isin(["CHF", "EUR", "USD","DKK","GBP"])),
            "Euro": pa.Column(float),
            "Boat Type": pa.Column(str),
            "Manufacturer": pa.Column(str,nullable=False),
            "Type": pa.Column(str,nullable=False),
            "Power": pa.Column(str,nullable=False),
            "Year Built": pa.Column(int, pa.Check.in_range(1800,current_year)),
            "Length": pa.Column(float, pa.Check.in_range(0,1000)),
            "Width": pa.Column(float, pa.Check.in_range(0,1000)),
            "Material": pa.Column(str,pa.Check.isin(material),nullable=False),
            "City": pa.Column(str),
            "Country": pa.Column(str),
            "Number of views last 7 days": pa.Column(int,pa.Check.in_range(0,10**6)),
        }
    )


    # header validation
    expected = set(schema.columns.keys())
    missing = expected - set(d.columns)
    if missing:
        print("WARNING: Missing columns:", missing)
        sys.exit(1)

    try:
        schema.validate(d)
        print("==> Data validated successfully.")
    except pa.errors.SchemaError as e:
        print("==> Data validation failed!")
        print(e)
        sys.exit(1)

    return d

###
def preprocess(input_path):

    logger.info(f"Data pre-processing is starting")
    df = preprocess_csv(input_path)
    df = transform_and_validate(df)
    logger.info(f"Data is cleaned")
    print(df.head())

    return df



###################################
# --->>>> PySpark section <<<---
###################################
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count

# Spark transforming...
def spark_transform(df,parquet_output_path=None,summary_output_path=None):
    spark = SparkSession.builder.appName("BoatDataETL").master("local[*]").getOrCreate()

    # Rename for Spark
    df = df.rename(columns={"Euro": "price_eur", "Country": "country"})
    spark_df = spark.createDataFrame(df)

    # Save to Parquet if requested
    if parquet_output_path:
        if os.path.isdir(parquet_output_path):
            # in case that this is directory, I still need a file
            parquet_output_path = os.path.join(parquet_output_path,"data.parquet")
        logger.info(f"Saving Spark DataFrame to {parquet_output_path}...")
        spark_df.write.mode("overwrite").parquet(parquet_output_path)

    if summary_output_path:
        if os.path.isdir(summary_output_path):
            # in case that this is directory, I still need a file
            summary_output_path = os.path.join(summary_output_path,"data_summary.csv")
        # do some summary
        summary_df = (
            spark_df.groupBy("country")
            .agg(avg("price_eur").alias("avg_price"), count("*").alias("count"))
            .orderBy("avg_price", ascending=False)
        )

        # Make sure directory exists
        os.makedirs(os.path.dirname(summary_output_path), exist_ok=True)
        
        summary_df.show(truncate=False)
        summary_df.toPandas().to_csv(summary_output_path, index=False)
        logger.info(f"Saved summary to {summary_output_path}")
    spark.stop()


###################################
# --->>>> Main <<<---
###################################
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL for boat sales.")
    parser.add_argument("-i", "--input", required=True, help="Path to the input file")
    parser.add_argument("-o", "--output", required=True, help="Path to the Parquet output file")
    parser.add_argument("-s", "--summary", required=True, help="Path to the summary CSV output file")
    parser.add_argument("--no-spark", action="store_true", help="Skip PySpark transformation")

    args = parser.parse_args()

    logger.info("Launching ETL pipeline...")

    df = preprocess(args.input)

    if not args.no_spark:
        spark_transform(df, parquet_output_path=args.output,summary_output_path=args.summary)











