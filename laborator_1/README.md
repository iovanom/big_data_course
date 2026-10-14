## Pornire
1. `docker compose up -d`  (Kafka UI: http://localhost:8080, mongo-express: http://localhost:8081)
2. `micromamba activate Playground`
3. Descărcați Yelp Open Dataset: review.json -> `data/raw/`, business.json + user.json -> `data/raw_dims/`
4. Rulați `notebooks/L01_yelp_bigdata.ipynb` (Restart & Run All). Etapa 4 pornește singură streaming-ul și producer-ul.
5. Dashboard: `streamlit run dashboard/app.py`
6. Demo live: `./scripts/reset_stream.sh`, apoi în terminale separate:
   `python scripts/streaming_job.py` și `python scripts/producer_kafka.py --pause 3`
