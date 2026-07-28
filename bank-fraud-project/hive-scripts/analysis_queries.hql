-- تشغّل الملف ده بعد ما تكون الداتا اتخزنت في HDFS عن طريق Spark
-- (يعني بعد تشغيل simulate_transactions.py لفترة كافية)

-- عدد المعاملات المشبوهة لكل منطقة
SELECT location, COUNT(*) AS fraud_count
FROM transactions
WHERE is_fraud = true
GROUP BY location
ORDER BY fraud_count DESC;

-- أكتر الحسابات تكرارًا في المعاملات المشبوهة
SELECT account_id, COUNT(*) AS fraud_count, SUM(amount) AS total_amount
FROM transactions
WHERE is_fraud = true
GROUP BY account_id
ORDER BY fraud_count DESC
LIMIT 10;

-- مقارنة عدد المعاملات العادية مقابل المشبوهة
SELECT is_fraud, COUNT(*) AS total
FROM transactions
GROUP BY is_fraud;
