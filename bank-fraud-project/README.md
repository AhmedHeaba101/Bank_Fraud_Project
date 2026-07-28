# Real-Time Banking Transaction Analysis & Fraud Detection

مشروع Big Data متكامل بيحاكي نظام كشف احتيال (Fraud Detection) لحظي على معاملات بنكية،
باستخدام Kafka وSpark Streaming وHadoop (HDFS) وHive، بالإضافة لـ FastAPI كنقطة دخول
للبيانات (Ingestion API).

## ⚡ عايز تخلص بسرعة؟ (الطريق الأسرع والمضمون)

لو الوقت ضيق، اتبع الترتيب ده بالظبط وسيب Hive جنب:

```bash
docker-compose up -d                      # 1) شغّل كل حاجة
docker logs -f kafka-init                 # 2) استنى Done. (Ctrl+C بعدها)
docker logs -f hdfs-init                  # 3) استنى Done. (Ctrl+C بعدها)
docker-compose up -d producer             # 4) شغّل المحاكاة (تولّد معاملات)
```
بعد كده شغّل الـ Spark Streaming job (سيبه شغال في الخلفية):
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  /opt/spark-jobs/fraud_detection_streaming.py
```
استنى دقيقتين لحد ما يتجمع داتا، وبعدين في نافذة تانية ولّد التقارير:
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/generate_reports.py
```
كده الـ API بقى جاهز يقدّم النتائج لـ Power BI (`http://localhost:8000/fraud-by-location`
وباقي الـ endpoints) من غير ما تحتاج Hive يكون شغال خالص. لو حابب تحدّث الأرقام بعد
ما تجمع معاملات أكتر، شغّل أمر `generate_reports.py` تاني وبس.

> **ليه مش استخدمنا Hive؟** صورة Hive (`bde2020`) بتاخد وقت طويل وأحيانًا بتتعطل في
> التهيئة الأولى، فمش مضمونة وقت العرض. المشروع لسه فيه Hive معرّف في الـ
> `docker-compose.yml` و`hive-scripts/` كجزء من الـ Architecture (طبقة Storage)،
> وتقدر تجرب تشغيله لو عندك وقت (قسم "خطوات التشغيل التفصيلية" تحت)، لكن الـ API
> والـ Power BI مش معتمدين عليه عشان نضمن إن العرض يشتغل 100%.

---

## الـ Architecture

```
[Simulator/Client] --HTTP--> [FastAPI] --> [Kafka] --> [Spark Streaming]
                                                              │
                                                              ▼
                                                        [Hadoop HDFS]
                                                              │
                                              ┌───────────────┴───────────────┐
                                              ▼                               ▼
                                        [Spark: generate_reports.py]      [Hive]
                                              │                        (اختياري)
                                              ▼
                                        [API: /fraud-by-location ...]
                                              │
                                              ▼
                                          [Power BI]
```

## مكونات المشروع

| المجلد | الوظيفة |
|---|---|
| `api/` | FastAPI service بتستقبل المعاملات وتبعتها لـ Kafka، وبتقدّم نتائج التحليل لـ Power BI |
| `init/` | سكريبتات Initialization بتجهز Kafka topic و HDFS و Hive تلقائيًا (تشتغل جوه الصور الجاهزة نفسها) |
| `producer/` | سكريبت محاكاة بيبعت معاملات عشوائية للـ API (demo) |
| `spark-jobs/` | `fraud_detection_streaming.py` (كشف الفراود لحظيًا) و `generate_reports.py` (توليد تقارير JSON لـ Power BI) |
| `hive-scripts/` | استعلامات Hive للتحليل فوق الداتا المخزنة (اختياري) |
| `reports/` | مجلد مشترك بيتحط فيه ملفات JSON اللي بيولّدها Spark، والـ API بيقرأها منه |
| `docker-compose.yml` | تعريف كل الخدمات (Kafka, Hadoop, Hive, Spark, API) |

## خطوات التشغيل التفصيلية

### 1. تشغيل كل الخدمات (فيه Initialization تلقائي)
```bash
docker-compose up -d
```
أول ما الخدمات تفتح، 3 containers بتجهز كل حاجة تلقائيًا وبعدين تقفل لوحدها (طبيعي، مش خطأ):
- **`kafka-init`**: بينشئ الـ Kafka topic (`bank-transactions`)
- **`hdfs-init`**: بينشئ مجلدات HDFS (`/data/transactions`, `/checkpoints/transactions`)
- **`hive-init`**: بينشئ جدول Hive (`transactions`)

> ملحوظة: الـ `producer` (المحاكاة) مايشتغلش تلقائي مع هذا الأمر عشان تقدر تشغّل
> الـ Spark job الأول — هتشغّله بنفسك في الخطوة 3.

تقدر تتابع كل واحد فيهم بـ:
```bash
docker logs -f kafka-init
docker logs -f hdfs-init
docker logs -f hive-init
```
لما تشوف رسالة `Done.` في آخر كل واحد، يبقى كل حاجة جاهزة، وممكن تشغّل الـ Spark job والمحاكاة على طول.

### 2. تشغيل الـ Spark Streaming job
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0 \
  /opt/spark-jobs/fraud_detection_streaming.py
```

### 3. تشغيل المحاكاة (توليد معاملات)
المحاكاة شغالة كـ container جاهز اسمه `producer` — مش محتاج تنصب Python على جهازك.
شغّله بالأمر:
```bash
docker-compose up -d producer
```
تقدر تتابع المعاملات اللي بتتبعت بـ:
```bash
docker logs -f producer
```

هتلاقي معاملات بتتبعت للـ API، وبعدين تدخل Kafka، وبعدين Spark بيحللها ويخزنها في HDFS.

### 4. توليد التقارير لـ Power BI (بدل استعلامات Hive الحية)
بعد ما تجمع كمية كافية من الداتا (سيب الـ producer شغال دقيقة أو اتنين):
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /opt/spark-jobs/generate_reports.py
```
السكريبت ده بيقرأ من HDFS مباشرة (مش محتاج Hive يكون شغال) وبيحفظ 4 ملفات JSON في
مجلد `reports/` — والـ API بيقدّمها فورًا لـ Power BI. شغّله تاني في أي وقت عشان تحدّث
الأرقام بعد ما تجمع معاملات جديدة.

> لو حابب تجرب Hive كمان (اختياري، مش لازم للعرض): بعد ما `hive-init` يخلص بنجاح،
> تقدر تشغّل `docker exec -it hive-server beeline -u "jdbc:hive2://localhost:10000" -f /scripts/analysis_queries.hql`

### 5. متابعة النتائج
- Hadoop UI: http://localhost:9870
- Spark UI: http://localhost:8080
- API Docs (Swagger): http://localhost:8000/docs

## اختبار الـ API يدويًا
```bash
curl -X POST http://localhost:8000/transaction \
  -H "Content-Type: application/json" \
  -d '{"account_id": "ACC0001", "amount": 7500, "transaction_type": "withdrawal", "location": "Cairo"}'
```

## ربط Power BI بالنتائج

بدل ما تعمل اتصال مباشر معقد بين Power BI و Hive (اللي بيحتاج تنصيب ODBC driver خاص)،
المشروع فيه **API endpoints جاهزة بترجع JSON**، وPower BI بيسحب منها مباشرة عن طريق
"Get Data from Web" — بسيط وسريع ومناسب جدًا للعرض في التدريب.

### الـ Endpoints المتاحة لـ Power BI

| Endpoint | الوصف |
|---|---|
| `GET /fraud-by-location` | عدد المعاملات المشبوهة لكل منطقة |
| `GET /top-fraud-accounts` | أكتر 10 حسابات تكرارًا في الفراود |
| `GET /fraud-vs-normal` | مقارنة عدد المعاملات العادية مقابل المشبوهة |
| `GET /all-transactions` | كل المعاملات بتفاصيلها (لعمل جداول وتقارير حرة) |

### خطوات الربط في Power BI Desktop

1. افتح **Power BI Desktop**
2. من الشريط العلوي: **Get Data → Web**
3. اكتب رابط الـ endpoint، مثلاً:
   ```
   http://localhost:8000/fraud-by-location
   ```
4. Power BI هيجيب الداتا كـ JSON وهيسألك تحول (Transform) لجدول — دوس **To Table**، وبعدها
   **Expand** على العمود عشان يفكّ الأعمدة (location, fraud_count)
5. كرر نفس الخطوات لباقي الـ endpoints (`top-fraud-accounts`, `fraud-vs-normal`, `all-transactions`)
   وكل واحد هيبقى Query/Table منفصل تقدر تربطهم ببعض لو احتجت
6. اعمل الـ visuals اللي تحبها:
   - **Bar chart**: عدد الفراود لكل منطقة (`fraud-by-location`)
   - **Table/Card**: أكتر الحسابات فيها فراود (`top-fraud-accounts`)
   - **Pie chart**: نسبة المعاملات العادية مقابل المشبوهة (`fraud-vs-normal`)

### تحديث تلقائي في Power BI Desktop (وقت الشغل المحلي)
لو عايز الداشبورد يفضل يتحدث لوحده وانت شغّال الـ simulator محليًا:
- في Power BI Desktop: **Home → Refresh** يدويًا وقت الـ demo
- أو من **Query Settings → Properties** فعّل خيار الـ refresh التلقائي كل فترة زمنية معينة

> ملحوظة: تأكد إن الـ `transaction-api` شغال (`docker ps`) وإنك شغّلت `generate_reports.py`
> على الأقل مرة واحدة قبل ما تسحب من الـ endpoints، وإلا هترجع رسالة "not found yet".

## نشر التقرير على Power BI Service (أونلاين) مع Auto-Refresh

النشر على Power BI Service (`app.powerbi.com`) بيخليك تشارك الداشبورد مع أي حد بلينك، وتضبط
تحديث تلقائي مجدول (**Scheduled Refresh**) بدل ما تفتح Power BI Desktop كل مرة.

### نقطة مهمة قبل ما تبدأ
الـ API عندك شغال على `localhost:8000` جوه جهازك، لكن Power BI Service سحابي (على سيرفرات
مايكروسوفت) ومش شايف جهازك أو الـ Docker containers بتاعتك مباشرة. عشان كده لازم تستخدم
**On-premises Data Gateway** — برنامج بيتنصب على جهازك وبيعمل "جسر" آمن بين Power BI Service
والـ API المحلي بتاعك.

```
[Power BI Service - سحابي] <--Gateway--> [جهازك: transaction-api على localhost:8000]
```

### الخطوات

**1. تنصيب الـ On-premises Data Gateway**
- نزّله من: https://learn.microsoft.com/power-bi/connect-data/service-gateway-onprem
- هتحتاج تسجّل دخول بنفس حساب Power BI بتاعك وقت التنصيب
- بعد التنصيب هتلاقي أيقونة "On-premises data gateway" شغالة في الخلفية على جهازك

**2. تسجيل الـ Gateway على حسابك**
- افتح تطبيق الـ Gateway المحلي، سجّل دخول بحسابك
- سمّي الـ Gateway (مثلاً `bank-fraud-gateway`)، وهيظهر تلقائي في إعدادات حسابك على
  `app.powerbi.com` تحت **Settings → Manage gateways**

**3. نشر التقرير من Desktop إلى Service**
- في Power BI Desktop بعد ما تبني الداشبورد: **Home → Publish**
- اختار الـ Workspace اللي عايز تنشر فيها
- بعد النشر هتلاقي رابط بيفتحلك التقرير على `app.powerbi.com`

**4. ربط الـ Dataset بالـ Gateway وضبط الجدولة**
- من `app.powerbi.com`، روح لـ **Workspace → Datasets → (اسم الداتا سيت) → Settings**
- تحت **Gateway connection**: فعّل واختار الـ Gateway اللي نصّبته
- تحت **Scheduled refresh**: فعّله واختار الفترة (مثلاً كل 30 دقيقة أو كل ساعة)
  وأقصى تكرار متاح في الخطة المجانية هو **8 مرات يوميًا**

**5. التأكد إن كل حاجة شغالة**
- لازم الـ `docker-compose` يفضل شغال على جهازك عشان الـ Gateway يقدر يوصل للـ API
- تقدر تجرب الـ refresh يدويًا الأول من **Datasets → Refresh now** وتتابع النتيجة

### بديل أسهل لو مش عايز تتعامل مع Gateway
لو الهدف بس إنك تعرض المشروع في التدريب، ممكن تستخدم **Personal Gateway** بدل الـ
Enterprise Gateway (خطوات مشابهة بس أبسط في الإعداد)، أو ببساطة تعرض الداشبورد بـ
Power BI Desktop مباشرة وقت العرض بدل النشر أونلاين — يفي بالغرض بنفس الكفاءة للتقديم.

## أفكار للتوسع (اختياري)
- إضافة موديل Machine Learning حقيقي (بدل القاعدة الثابتة) باستخدام Spark MLlib
- استضافة الـ API على سيرفر سحابي (بدل localhost) عشان تلغي الحاجة للـ Gateway بالكامل
- إضافة Row-Level Security في Power BI لو الداشبورد هيتشارك مع فريق
