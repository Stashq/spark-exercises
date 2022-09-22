import pandas as pd
from tqdm import tqdm
import csv
import random
import string
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os
import math

random.seed(1999)

letters = string.ascii_lowercase
letters_upper = string.ascii_uppercase
for _i in range(0, 10):
    letters += letters

for _i in range(0, 10):
    letters += letters_upper


def random_string(stringLength=10):
    """Generate a random string of fixed length"""
    return "".join(random.sample(letters, stringLength))


def generate_3fields_csv(start, end, name, low, high, col_names):
    rows = []
    for p in tqdm(range(start, end)):
        rows.append([p, f"{name}_{p}", random.randint(low, high)])
    #   Save dataframe
    df = pd.DataFrame(rows, columns=col_names)
    df.to_csv(f"{name}/{start}-{end-1}.csv", index=False)


def generate_3fields_many_csv(
    start, end, name, low, high, col_names, max_n_rows=40000000
):
    os.makedirs(name, exist_ok=True)
    for i in range(math.ceil((end - start)/max_n_rows)):
        sub_start = start + i * max_n_rows
        sub_end = sub_start + max_n_rows
        if sub_end > end:
            sub_end = end
        generate_3fields_csv(sub_start, sub_end, name, low, high, col_names)


# print("Products between {} and {}".format(1, 75000000))
product_ids = [x for x in range(1, 75000000)]
dates = [
    "2020-07-01",
    "2020-07-02",
    "2020-07-03",
    "2020-07-04",
    "2020-07-05",
    "2020-07-06",
    "2020-07-07",
    "2020-07-08",
    "2020-07-09",
    "2020-07-10",
]
seller_ids = [x for x in range(1, 10)]


# #   Generate products
# products = [[0, "product_0", 22]]
# for p in tqdm(product_ids):
#     products.append([p, "product_{}".format(p), random.randint(1, 150)])
# #   Save dataframe
# df = pd.DataFrame(products)
# df.columns = ["product_id", "product_name", "price"]
# df.to_csv("products.csv", index=False)
# del df
# del products

generate_3fields_many_csv(
    start=0, end=75000000, name="product", low=1, high=150,
    col_names=["product_id", "product_name", "price"])

# #   Generate sellers
# sellers = [[0, "seller_0", 2500000]]
# for s in tqdm(seller_ids):
#     sellers.append(
#       [s, "seller_{}".format(s), random.randint(12000, 2000000)])
# #   Save dataframe
# df = pd.DataFrame(sellers)
# df.columns = ["seller_id", "seller_name", "daily_target"]
# df.to_csv("sellers.csv", index=False)

generate_3fields_many_csv(
    start=0, end=2500000, name="seller", low=12000, high=2000000,
    col_names=["seller_id", "seller_name", "daily_target"])

#   Generate sales
total_rows = 500000
prod_zero = int(total_rows * 0.95)
prod_others = total_rows - prod_zero + 1
df_array = [
    ["order_id", "product_id", "seller_id", "date", "num_pieces_sold",
     "bill_raw_text"]
]
with open("sales.csv", "w", newline="") as f:
    csvwriter = csv.writer(f)
    csvwriter.writerows(df_array)

order_id = 0
for i in tqdm(range(0, 40)):
    df_array = []

    for i in range(0, prod_zero):
        order_id += 1
        df_array.append(
            [
                order_id,
                0,
                0,
                random.choice(dates),
                random.randint(1, 100),
                random_string(500),
            ]
        )

    with open("sales.csv", "a", newline="") as f:
        csvwriter = csv.writer(f)
        csvwriter.writerows(df_array)

    df_array = []
    for i in range(0, prod_others):
        order_id += 1
        df_array.append(
            [
                order_id,
                random.choice(product_ids),
                random.choice(seller_ids),
                random.choice(dates),
                random.randint(1, 100),
                random_string(500),
            ]
        )

    with open("sales.csv", "a", newline="") as f:
        csvwriter = csv.writer(f)
        csvwriter.writerows(df_array)

print("Done")

spark = (
    SparkSession.builder.master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", -1)
    .appName("Exercise1")
    .getOrCreate()
)

products = spark.read.csv("products.csv", header=True, mode="DROPMALFORMED")
products.show()
products.write.parquet("products_parquet", mode="overwrite")

sales = spark.read.csv("sales.csv", header=True, mode="DROPMALFORMED")
sales.show()
sales.repartition(200, col("product_id")).write.parquet(
    "sales_parquet", mode="overwrite"
)

sellers = spark.read.csv("sellers.csv", header=True, mode="DROPMALFORMED")
sellers.show()
sellers.write.parquet("sellers_parquet", mode="overwrite")