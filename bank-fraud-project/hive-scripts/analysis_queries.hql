SELECT location, COUNT(*) AS fraud_count
FROM transactions
WHERE is_fraud = true
GROUP BY location
ORDER BY fraud_count DESC;

SELECT account_id, COUNT(*) AS fraud_count, SUM(amount) AS total_amount
FROM transactions
WHERE is_fraud = true
GROUP BY account_id
ORDER BY fraud_count DESC
LIMIT 10;

SELECT is_fraud, COUNT(*) AS total
FROM transactions
GROUP BY is_fraud;
