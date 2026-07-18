# نبدأ من صورة Airflow الرسمية إصدار 2.10.0
FROM apache/airflow:2.10.0

# التبديل لمستخدم root لتسطيب برامج النظام (Java)
USER root

# تحديث النظام وتسطيب Java (مطلوب عشان PySpark يشتغل)
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# ضبط متغير البيئة الخاص بجافا
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# العودة لمستخدم airflow لتسطيب مكتبات بايثون
USER airflow

# تسطيب PySpark
RUN pip install --no-cache-dir pyspark==3.5.1
