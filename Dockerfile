FROM quay.io/astronomer/astro-runtime:11.4.0

RUN pip install apache-airflow-providers-amazon
COPY . .
# Create the .kaggle directory
RUN mkdir -p home/astro/.kaggle
COPY kaggle.json home/astro/.kaggle 
RUN pip install kaggle
RUN pip install apache-airflow-providers-amazon
RUN pip install --upgrade cryptography pyopenssl
RUN pip install --upgrade sqlalchemy pandas
RUN pip install mysql-connector-python
RUN pip install psycopg2-binary
# RUN pip install --user --upgrade pip
RUN pip install -r requirements.txt
