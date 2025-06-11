# Realtime-Transaction-Analysis 

### 1. Introduction
The system uses Python to generate and preprocess input data, PostgreSQL for reliable data storage, and Kafka to transmit real-time data streams. Spark Streaming handles processing and analysis of the data as it arrives, while Streamlit provides an interactive interface for users to monitor results. All components are packaged with Docker Compose for easy deployment and management as independent containers.

### 2. System Architecture

![image](https://github.com/user-attachments/assets/0d73e1cd-dda2-497c-a7b3-de3e19f3c58b)

### 3. System Components
- main.py: This is the main Python script that creates the transactional data , it also creates the Kafka topic . It also contains the logic to consume the data from the Kafka topic and produce data to financial_transactions topic on Kafka.
- spark-streaming.py: This is the Python script that contains the logic to consume the data from the Kafka topic (financial_transactions) and aggregate the data and produce data to specific topics on Kafka.
- streamlit-app.py: This is the Python script that contains the logic to consume the aggregated transaction data from the Kafka topic and display the transaction data in realtime using Streamlit.

### 4. Setting up the System
##### Steps to Run
1. Clone this repository.
2. Navigate to the root containing the Docker Compose file.
3. Run the following command:
```
docker-compose up -d
```
This command will start Zookeeper, Kafka and Postgres containers in detached mode (-d flag). Kafka will be accessible at localhost:9092 and Postgres at localhost:6432.

##### Running the App

1. Install the required Python packages using the following command:
 ```
pip install -r requirements.txt
```
2. Creating the data and generating transactions information on Kafka topic:
```
python main.py
```
3. Consuming the transaction data from Kafka topic and producing data to specific topics on Kafka:
```
python spark-streaming.py
```
4. Running the Streamlit app:
```
streamlit run streamlit-app.py
``` 

### 5. Dashboard

![Doanh thu theo hang](https://github.com/user-attachments/assets/2802f89e-2172-4f40-8aa3-ecd11e47bcc4)

![Ảnh chụp màn hình 2025-06-10 190602](https://github.com/user-attachments/assets/5ab3a7a2-378c-4958-a907-1ce15eef3bf7)

![Ảnh chụp màn hình 2025-06-10 190837](https://github.com/user-attachments/assets/b46a27f7-90fa-4d39-b298-e3672d13a6d0)

