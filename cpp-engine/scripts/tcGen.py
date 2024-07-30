import random
from collections import defaultdict

# Configuration
num_threads = 40
tickers = random.randint(2, 500)
num_orders_per_thread = random.randint(10000, 50000) * tickers;
tickers = ["A" + str(i) for i in range(1, tickers)]
max_price = 1000000
max_quantity = 100000
order_id = 1


print(num_threads)
for thread_id in range(num_threads):
    print(f"{thread_id} o")

thread_order_mapping = {} 
order_thread_map = defaultdict(list)
for _ in range(num_orders_per_thread):
    ticker = random.choice(tickers)
    price = random.randint(1, max_price)
    quantity = random.randint(1, max_quantity)
    order_type = "B" if random.random() < 0.5 else "S"
    thread_id = random.randint(0, num_threads - 1)
    if random.random() < 0.27 and len(order_thread_map[thread_id]) > 0:
        print(f"{thread_id} C {random.choice(order_thread_map[thread_id])}")

    print(f"{thread_id} {order_type} {order_id} {ticker} {price} {quantity}")
    thread_order_mapping[order_id] = thread_id
    order_thread_map[thread_id].append(order_id)
    order_id += 1

for thread_id in range(num_threads):
    print(f"{thread_id} x")
