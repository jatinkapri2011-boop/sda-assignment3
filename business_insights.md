# Business Insight Write-up — Assignment 3

## Objective
The dashboard converts the Kafka retail order stream into business-facing insights. Kafka carries the events, `consumer.py` consumes them, MongoDB Atlas stores them for analytics, and Streamlit presents a live dashboard.

## Initial sample-data observations
Using the 10 records supplied in Assignment 2:

- Total sales value: **₹25,990**
- Total units: **20**
- Total orders: **10**
- Average order value: **₹2,599**
- Highest sales value by payment method: **CARD — ₹9,297**
- Highest sales value by product: **P505 — ₹9,898**

## Business interpretation
1. **Payment behaviour:** Card contributes the largest sales value in the sample, followed by UPI and COD. This can help management monitor payment preference and ensure the checkout process supports the most-used methods.
2. **Product concentration:** P505 contributes the highest sales value in the initial data. A live dashboard makes it possible to monitor whether this concentration continues as more orders arrive.
3. **Order economics:** The starting average order value is ₹2,599. This metric can be tracked over time to identify changes in basket size and revenue quality.
4. **Real-time monitoring:** Because the consumer continuously moves Kafka events into MongoDB Atlas and the dashboard auto-refreshes, managers can observe changes without rebuilding a static report.

## Important data note
The current Assignment 2 sample contains one event per order. If the system is later expanded to emit multiple lifecycle events for the same order, revenue must be calculated at the order level or from the appropriate completed-payment event to avoid double counting. The dashboard therefore de-duplicates on `order_id` for KPI and product/payment analysis.

## Recommended managerial actions
- Monitor payment-method mix and investigate sudden changes.
- Track high-value products for inventory and promotion decisions.
- Watch average order value as a leading indicator of basket-size movement.
- Add operational alerts later for unusually high/low sales or payment failures.
