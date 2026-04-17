import streamlit as st
import time
import os
import pandas as pd
import random

st.set_page_config(page_title="Streaming Dashboard", layout="wide")
st.title("⚡ Crypto Real-Time Streaming Dashboard")

DATA_PATH = "dashboard/streaming/sample_data"

placeholder = st.empty()

while True:
    try:
        if os.path.exists(DATA_PATH):
            # Read parquet safely
            pdf = pd.read_parquet(DATA_PATH)

            if not pdf.empty:

                # ---- Simulate "recent streaming slice" ----
                pdf = pdf.tail(200)  # take recent chunk
                pdf = pdf.sample(min(len(pdf), random.randint(20, 80)))

        else:
            pdf = None

        with placeholder.container():
            st.subheader("📡 Live Windowed Metrics")

            if pdf is not None and not pdf.empty:

                # ---- Handle Spark window struct ----
                if "window" in pdf.columns:
                    pdf["window_start"] = pdf["window"].apply(
                        lambda x: x["start"] if isinstance(x, dict) else None
                    )
                    pdf["window_end"] = pdf["window"].apply(
                        lambda x: x["end"] if isinstance(x, dict) else None
                    )
                else:
                    pdf["window_start"] = None
                    pdf["window_end"] = None

                col1, col2 = st.columns(2)

                col1.metric("Avg Price", round(pdf["avg_price"].mean(), 2))
                col2.metric("Records", len(pdf))

                # ---- Sort for time-series feel ----
                if "window_start" in pdf.columns and pdf["window_start"].notna().any():
                    chart_df = (
                        pdf.sort_values("window_start")
                           .set_index("window_start")["avg_price"]
                    )
                    st.line_chart(chart_df)

                st.dataframe(pdf.tail(10))

                st.caption("⚡ Simulated live updates every 5 seconds")

            else:
                st.warning("Waiting for streaming data...")

        time.sleep(5)

    except Exception as e:
        st.error(f"Error: {e}")
        time.sleep(5)