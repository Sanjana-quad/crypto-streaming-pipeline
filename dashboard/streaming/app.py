import streamlit as st
import time
import os
from pyspark.sql import SparkSession
from src.utils.config_loader import load_config

st.set_page_config(page_title="Streaming Dashboard", layout="wide")
st.title("⚡ Crypto Real-Time Streaming Dashboard")

DATA_PATH = load_config()["paths"]["stream_output"]

@st.cache_resource
def get_spark():
    return SparkSession.builder.getOrCreate()

spark = get_spark()
placeholder = st.empty()

while True:
    try:
        if os.path.exists(DATA_PATH):
            df = spark.read.parquet(DATA_PATH).limit(1000)
            pdf = df.toPandas()
        else:
            pdf = None

        with placeholder.container():
            st.subheader("📡 Live Windowed Metrics")

            if pdf is not None and not pdf.empty:

                pdf["window_start"] = pdf["window"].apply(lambda x: x["start"])
                pdf["window_end"] = pdf["window"].apply(lambda x: x["end"])

                col1, col2 = st.columns(2)

                col1.metric("Avg Price", round(pdf["avg_price"].mean(), 2))
                col2.metric("Records", len(pdf))

                st.line_chart(
                    pdf.sort_values("window_start")
                       .set_index("window_start")["avg_price"]
                )

                st.dataframe(pdf.tail(10))

            else:
                st.warning("Waiting for streaming data...")

        time.sleep(5)

    except Exception as e:
        st.error(f"Error: {e}")
        time.sleep(5)
