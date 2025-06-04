import time
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import simplejson as json
import streamlit as st
from kafka import KafkaConsumer
from streamlit_autorefresh import st_autorefresh
import psycopg2
import matplotlib.cm as cm
import matplotlib.dates as mdates
import streamlit as st



def create_kafka_consumer(topic_name):
    # Set up a Kafka consumer with specified topic and configurations
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    return consumer

def fetch_data_from_kafka(consumer):
    # Poll Kafka consumer for messages within a timeout period
    messages = consumer.poll(timeout_ms=1000)
    data = []

    # Extract data from received messages
    for message in messages.values():
        for sub_message in message:
            data.append(sub_message.value)
    return data

def fetch_voting_stats():
    # Connect to PostgreSQL database
    connection = psycopg2.connect(
        host= "localhost",
        port= "6432",
        database= "postgres",
        user= "postgres1",
        password= "postgres12"
    )
    cursor = connection.cursor()

    # Fetch total number of voters
    query = 'SELECT "productBrand",count(*) FROM transactions12 GROUP BY "productBrand"'
    cursor.execute(query)
    records = cursor.fetchall()
    return records

def plot_revenue_line_chart(df: pd.DataFrame, groupBy,title="Doanh thu theo ngày (Line Chart)"):

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(df[groupBy], df['total_sales'], marker='o', linestyle='-', color='blue')

    ax.set_xlabel('Ngày')
    ax.set_ylabel('Doanh thu')
    ax.set_title(title)

    # Tùy chỉnh nhãn trục X cho dễ nhìn
    ax.tick_params(axis='x', rotation=45)
    ax.ticklabel_format(style='plain', axis='y') 

    plt.tight_layout()

    return fig

def plot_revenue_line_chart_hour(df: pd.DataFrame, groupBy, title="Doanh thu theo giờ (Line Chart)"):
    # Đảm bảo cột groupBy là datetime
    df[groupBy] = pd.to_datetime(df[groupBy])

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(df[groupBy], df['total_sales'], marker='o', linestyle='-', color='blue')

    # Hiển thị theo ngày và giờ, không có tháng
    ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%d %H:%M'))  # ví dụ: 13 14:00

    ax.set_xlabel('Ngày')
    ax.set_ylabel('Doanh thu')
    ax.set_title(title)

    ax.tick_params(axis='x', rotation=45)
    ax.ticklabel_format(style='plain', axis='y')

    plt.tight_layout()
    return fig

def plot_revenue_pie_chart(df: pd.DataFrame,groupBy, title="Tỷ lệ doanh thu các hãng điện thoại"):
    fig, ax = plt.subplots(figsize=(8, 6))

    # Tính phần trăm
    total = df['total_sales'].sum()
    labels_with_pct = [
        f"{brand} – {sales / total:.1%}" for brand, sales in zip(df[groupBy], df['total_sales'])
    ]

    # Tạo màu riêng cho từng hãng (sử dụng colormap)
    cmap = cm.get_cmap('tab20', len(df))  # 'tab20' có nhiều màu hơn
    colors = [cmap(i) for i in range(len(df))]

    # Vẽ pie chart
    wedges, texts = ax.pie(
        df['total_sales'],
        labels=None,
        colors=colors,
        startangle=90
    )

    ax.axis('equal')

    # Legend với phần trăm
    ax.legend(
        wedges,
        labels_with_pct,
        title="Hãng",
        loc="center left",
        bbox_to_anchor=(1, 0.5)
    )

    plt.title(title)
    plt.tight_layout()
    return fig

def plot_revenue_bar_chart(df: pd.DataFrame, groupBy,title="Doanh thu theo hãng (Bar Chart)"):
    fig, ax = plt.subplots()
    ax.bar(df[groupBy], df['total_sales'], color='skyblue')
    ax.set_ylabel('Doanh thu (USD)')
    ax.set_title(title)
    ax.set_xticklabels(df[groupBy], rotation=45, ha='right')  # Xoay nhãn nếu dài
    return fig

def update_data():
    # Placeholder to display last refresh time
    last_refresh = st.empty()
    last_refresh.text(f"Last refreshed at: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    

    # DAY

    consumer = create_kafka_consumer("sales_per_day")
    data = fetch_data_from_kafka(consumer)
    results = pd.DataFrame(data)
    results = results.loc[results.groupby('transaction_date')['total_sales'].idxmax()]
    st.title("Doanh Thu theo ngày ")

    st.dataframe(results)
    
    bar_fig = plot_revenue_line_chart(results , 'transaction_date', "Doanh thu theo ngày (Line Chart)")
    st.pyplot(bar_fig)

    st.title("DOANH THU THEO HÃNG")    
    consumer_cate = create_kafka_consumer("sales_per_category")
    data_cate = fetch_data_from_kafka(consumer_cate)
    result_cate = pd.DataFrame(data_cate)
    result_cate = result_cate.loc[result_cate.groupby('productBrand')['total_sales'].idxmax()]
    
    col3, col4 = st.columns(2)

    with col3:
        st.dataframe(result_cate)

    # BRAND 

    circle_fig = plot_revenue_pie_chart(result_cate,"productBrand" ,"Revenue by Brand")
    bar_fig2 = plot_revenue_bar_chart(result_cate, "productBrand","Revenue by Brand (Bar Chart)")
    with col4:
        st.pyplot(circle_fig)
        st.pyplot(bar_fig2)

    # PAYMENT

    st.title("DOANH THU THEO PHƯƠNG THỨC THANH TOÁN")

    consumer_payemnt = create_kafka_consumer("sales_per_payment")
    data_payment = fetch_data_from_kafka(consumer_payemnt)
    result_payment = pd.DataFrame(data_payment)
    result_payment = result_payment.loc[result_payment.groupby('payment_method')['total_sales'].idxmax()]
    col5, col6 = st.columns(2)
    with col5:
        st.dataframe(result_payment)

    circle_fig_payment = plot_revenue_pie_chart(result_payment,"payment_method", "Revenue by Payment Method")
    bar_fig_payment = plot_revenue_bar_chart(result_payment,"payment_method", "Revenue by Payment Method (Bar Chart)")
    with col6:
        st.pyplot(circle_fig_payment)
        st.pyplot(bar_fig_payment)

    # HOUR  
    
    
    st.title("DOANH THU THEO TỪNG GIỜ")
    consumer_hour = create_kafka_consumer("sales_per_hour")
    data_hour = fetch_data_from_kafka(consumer_hour)
    result_hour = pd.DataFrame(data_hour)
    result_hour = result_hour.loc[result_hour.groupby('transaction_hour')['total_sales'].idxmax()]
    st.dataframe(result_hour)
    result_hour['transaction_hour'] = pd.to_datetime(result_hour['transaction_hour'])

    result_hour['hour_range'] = result_hour['transaction_hour'].apply(
        lambda dt: f"{dt.hour}:00 - {dt.hour + 1}:00"
    )
    line_fig_hour = plot_revenue_line_chart_hour(result_hour, 'transaction_hour', "Doanh thu theo giờ (Line Chart)")
    st.dataframe(result_hour[['hour_range', 'total_sales']])
    st.pyplot(line_fig_hour)

def sidebar():
    # Initialize last update time if not present in session state
    if st.session_state.get('last_update') is None:
        st.session_state['last_update'] = time.time()

    # Slider to control refresh interval
    refresh_interval = st.sidebar.slider("Refresh interval (seconds)", 5, 60, 10)
    st_autorefresh(interval=refresh_interval * 1000, key="auto")

    # Button to manually refresh data
    if st.sidebar.button('Refresh Data'):
        update_data()

st.title('THỐNG KÊ DOANH THU')


sidebar()
update_data()