import sys
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from pymongo import MongoClient

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from src.lab_common import MONGO_COLL, MONGO_DB, MONGO_URI

st.set_page_config(page_title="Yelp streaming", layout="wide")
st.title("Yelp — recenzii procesate în timp (aproape) real")


@st.cache_resource
def get_client():
    return MongoClient(MONGO_URI)


@st.fragment(run_every="5s")
def live():
    docs = list(get_client()[MONGO_DB][MONGO_COLL].find({}, {"_id": 0}).sort("window_start", 1))
    if not docs:
        st.info("Nicio fereastră în MongoDB încă. Porniți streaming_job + producer.")
        return
    df = pd.DataFrame(docs)
    last = df.iloc[-1]
    c1, c2, c3 = st.columns(3)
    c1.metric("Total recenzii procesate", f"{int(df['n_reviews'].sum()):,}")
    c2.metric("Media stelelor (ultima fereastră)", f"{last['avg_stars']:.2f}")
    c3.metric("Ferestre", len(df))
    st.plotly_chart(px.line(df, x="window_start", y="n_reviews", markers=True,
                            title="Recenzii pe fereastră (event time)"), use_container_width=True)
    st.dataframe(df.sort_values("window_start", ascending=False).head(10), use_container_width=True)
    st.caption(f"Actualizat: {pd.Timestamp.now(tz='UTC'):%Y-%m-%d %H:%M:%S} UTC")


live()
