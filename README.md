![image](https://github.com/user-attachments/assets/0d73e1cd-dda2-497c-a7b3-de3e19f3c58b)# Realtime-Transaction-Analysis 

### 1. Introduction
The system uses Python to generate and preprocess input data, PostgreSQL for reliable data storage, and Kafka to transmit real-time data streams. Spark Streaming handles processing and analysis of the data as it arrives, while Streamlit provides an interactive interface for users to monitor results. All components are packaged with Docker Compose for easy deployment and management as independent containers.

### 2. System Architecture


### 3. System Components
- main.py: This is the main Python script that creates the transactional data , it also creates the Kafka topic . It also contains the logic to consume the votes from the Kafka topic and produce data to financial_transactions topic on Kafka.
- spark-streaming.py: This is the Python script that contains the logic to consume the votes from the Kafka topic (financial_transactions) and aggregate the votes and produce data to specific topics on Kafka.
- streamlit-app.py: This is the Python script that contains the logic to consume the aggregated voting data from the Kafka topic and display the voting data in realtime using Streamlit.

### 4. Setting up the System
##### Steps to Run
1. Clone this repository.
2. Navigate to the root containing the Docker Compose file.
3. Run the following command:
``` docker-compose up -d ```
This command will start Zookeeper, Kafka and Postgres containers in detached mode (-d flag). Kafka will be accessible at localhost:9092 and Postgres at localhost:6432.

##### Running the App
1. Install the required Python packages using the following command:
2. pip install -r requirements.txt
3. Creating the data and generating transactions information on Kafka topic:
``` python main.py ```
4. Consuming the transaction data from Kafka topic and producing data to specific topics on Kafka:
``` python spark-streaming.py ```
5. Running the Streamlit app:
``` streamlit run streamlit-app.py ``` 

### 5. Dashboard



