# SDA Assignment 3 — Live Retail Streaming Dashboard

This project extends Assignment 2's `orders-topic` Kafka stream into a live analytics dashboard.

## Architecture

`producer.py / producer_live.py` → **Kafka `orders-topic`** → `consumer.py` → **MongoDB Atlas** → `dashboard.py (Streamlit)`

## Files

- `consumer.py` — consumes the Kafka topic and upserts events into MongoDB Atlas.
- `dashboard.py` — live Streamlit dashboard with KPIs, filters and 4 charts.
- `producer_live.py` — optional continuous producer for demo/testing.
- `sample_data.csv` — Assignment 2 sample data, retained for reproducibility.
- `business_insights.md` — business insight write-up for submission.
- `.env.example` — configuration template; do **not** commit real credentials.
- `requirements.txt` — Python dependencies.

## MongoDB Atlas setup

1. Create a free MongoDB Atlas cluster.
2. Create a database user and password.
3. Add your current IP address under Network Access.
4. Copy the Python connection string.
5. Create a local `.env` file from `.env.example` and set `MONGODB_URI`.

Example database settings:

- Database: `retail_streaming`
- Collection: `orders`

Do not put your real MongoDB password into GitHub.

## Run the dashboard

### Terminal 1 — start Kafka
Use the same Kafka setup from Assignment 2 and make sure the broker is available at `localhost:9092`.

### Terminal 2 — start the consumer

```bash
pip install -r requirements.txt
python consumer.py
```

### Terminal 3 — start the producer
For the original Assignment 2 ten-record stream:

```bash
python producer.py
```

For a continuously changing live demo:

```bash
python producer_live.py
```

### Terminal 4 — start the dashboard

```bash
streamlit run dashboard.py
```

The browser dashboard auto-refreshes every few seconds and reads the latest records from MongoDB Atlas.

## Dashboard contents

1. Sales value over time — line chart
2. Sales value by payment method — bar chart
3. Product-wise sales value — bar chart
4. Event type mix — donut chart

It also includes Sales Value, Orders, Units, Average Order Value and Last Event KPIs, along with date/payment/product filters.

## Submission checklist

- [ ] `consumer.py` is visibly consuming `orders-topic`.
- [ ] MongoDB Atlas contains records in `retail_streaming.orders`.
- [ ] Streamlit dashboard shows at least 3 charts.
- [ ] Auto-refresh demonstrates live updates.
- [ ] Business insights are included in `business_insights.md`.
- [ ] Screenshots are taken showing the dashboard and consumer terminal.
- [ ] `.env` is **not** committed to GitHub.
