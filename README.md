# Basket Craft Dashboard

**Live app:** https://basket-craft-dashboard.streamlit.app/

A Streamlit dashboard connected to a Snowflake data warehouse.

## Features

- **Headline Metrics** — total revenue, orders, average order value, and items sold with month-over-month deltas
- **Revenue Trend** — monthly revenue area chart with a date filter (last 3/6/12 months, all time, or custom range)
- **Top Products by Revenue** — bar chart respecting the date filter
- **Bundle Finder** — pick any product and see which products are most frequently bought alongside it

## Running locally

1. Create and activate a virtual environment:
   ```bash
   python -m venv .venv
   .venv/Scripts/activate
   ```
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. Create a `.env` file with your Snowflake credentials:
   ```
   SNOWFLAKE_ACCOUNT=...
   SNOWFLAKE_USER=...
   SNOWFLAKE_PASSWORD=...
   SNOWFLAKE_ROLE=...
   SNOWFLAKE_WAREHOUSE=...
   SNOWFLAKE_DATABASE=...
   SNOWFLAKE_SCHEMA=...
   ```
4. Run the app:
   ```bash
   streamlit run app.py
   ```
