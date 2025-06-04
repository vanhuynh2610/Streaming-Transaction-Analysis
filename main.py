from faker import Faker
from confluent_kafka import SerializingProducer
import random
from datetime import datetime
import time
import json

fake = Faker()

import random
from faker import Faker
import datetime

fake = Faker()

products = {
    'SM-A556-128-BLK': ['Samsung A55', 'Samsung', 10000, '128GB', 'Black'],
    'MGMT3-256-BLU': ['iPhone 12 Pro Max', 'Apple', 15000, '256GB', 'Pacific Blue'],
    'MZB0A1Z-128-GRN': ['Xiaomi Mi 11', 'Xiaomi', 8000, '128GB', 'Green'],
    'OP-NE211-256-BLK': ['OnePlus 9', 'OnePlus', 12000, '256GB', 'Black'],
    'GP-PIX6-128-GRY': ['Google Pixel 6', 'Google', 7000, '128GB', 'Stormy Black'],
    'OP-REN6-128-SLV': ['Oppo Reno 6', 'Oppo', 6500, '128GB', 'Silver'],
    'SN-XP5II-256-BLK': ['Sony Xperia 5 II', 'Sony', 14000, '256GB', 'Black'],
    'RM-8PRO-128-BLU': ['Realme 8 Pro', 'Realme', 6000, '128GB', 'Blue'],
    'VV-V21-128-PRL': ['Vivo V21', 'Vivo', 5500, '128GB', 'Pearl'],
    'MT-GPWR-128-BLK': ['Motorola Moto G Power', 'Motorola', 5000, '128GB', 'Black'],
    'AS-ROG5-256-WHT': ['Asus ROG Phone 5', 'Asus', 32000, '256GB', 'White'],
    'LG-VLT-128-PRP': ['LG Velvet', 'LG', 9000, '128GB', 'Aurora Purple'],
    'TCL-10PRO-128-GRN': ['TCL 10 Pro', 'TCL', 9000, '128GB', 'Green'],
    'SM-S901-256-BLK': ['Samsung Galaxy S21', 'Samsung', 12000, '256GB', 'Black'],
    'SM-S902-256-BLK': ['Samsung Galaxy S23', 'Samsung', 14000, '256GB', 'Black'],
    'SM-S903-128-WHT': ['Samsung Galaxy S24', 'Samsung', 16500, '128GB', 'White'],
    'A2777-128-WHT': ['iPhone 13', 'Apple', 13000, '128GB', 'White'],
    'A2774-128-WHT': ['iPhone 14', 'Apple', 14000, '128GB', 'White'],
    'A2775-128-WHT': ['iPhone 15', 'Apple', 15000, '128GB', 'White'],
    'A2776-128-WHT': ['iPhone 16', 'Apple', 16000, '128GB', 'White'],
    'MZB0B2Z-128-RED': ['Xiaomi Redmi Note 10', 'Xiaomi', 6000, '128GB', 'Red'],
    'OP-NORD-128-BLU': ['OnePlus Nord', 'OnePlus', 7500, '128GB', 'Blue'],
    'GP-PIX5-128-GRN': ['Google Pixel 5', 'Google', 9000, '128GB', 'Green'],
    'OP-FX3-256-BLK': ['Oppo Find X3', 'Oppo', 18000, '256GB', 'Black'],
    'RM-NAR30-64-BLU': ['Realme Narzo 30', 'Realme', 4500, '64GB', 'Blue'],
    'VV-X60-128-BLK': ['Vivo X60', 'Vivo', 9500, '128GB', 'Black'],
    'MT-EDG20-128-GRY': ['Motorola Edge 20', 'Motorola', 13000, '128GB', 'Grey'],
    'LG-WING-128-BLU': ['LG Wing', 'LG', 15000, '128GB', 'Blue'],
}
def generate_sales_transactions():
    user = fake.simple_profile()
    product_id = random.choice(list(products.keys()))
    product_infor = products[product_id]
    quantity = random.randint(1,10)
    return {
        "transactionId" : fake.uuid4(),
        "productId" : product_id,
        "productName" : product_infor[0],
        "productBrand" :product_infor[1],
        "productPrice" : product_infor[2],
        "productQuantity" : quantity,
        "customerId": user["username"],
        "transactionDate": datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'paymentMethod': random.choice(['credit_card', 'debit_card', 'cash']),
    }

def delivery_report(err,msg):
    if err is not None:
        print(f'Delivery Failed : {err}')
    else:
        print(f'Message delivered to {msg.topic} [{msg.partition()}]')

def main():
    topic = 'financial_transactions'
    producer = SerializingProducer({
        'bootstrap.servers':'localhost:9092'
    })

    curr_time = datetime.datetime.now()
    while (datetime.datetime.now() - curr_time).seconds < 120:
        try:
            transaction = generate_sales_transactions()
            transaction['totalPrice'] = transaction['productPrice'] * transaction['productQuantity']
            print(transaction)

            producer.produce(topic,
                             key=transaction['transactionId'],
                             value=json.dumps(transaction),
                             on_delivery=delivery_report)
            producer.poll(0)
            time.sleep(5)

        except BufferError:
            print("Buffer full waiting .... ")
            time.sleep(1)
        except Exception as e:
            print(e)
    
if __name__ == "__main__":
    main()