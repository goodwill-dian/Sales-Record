from sales.models import Region, Country, ItemType, Order, BaseModel
import csv
import os, io
import pandas as pd
# import numpy as np
# import tempfile
import uuid
from datetime import datetime
import itertools
import threading
from django.db import transaction, connections
from django.utils import timezone
# from multiprocessing import Pool, Lock
import multiprocessing


"""Prototype 2 & Prototype 2 --> using Pool Works"""

"""Prototype 2 --> using Pool"""
# def run():
#     # with transaction.atomic():
#     with open('scripts/csv_files/5m_Sales_Records.csv') as file:
#         csv_reader = csv.reader(file)
#         next(csv_reader) 

#         start_time = timezone.now()

#         region_list = []
#         country_list = []
#         item_type_list = []
#         orders_list = []
#         i=1
#         for row in csv_reader:
#             # print(row[5])
#             # break
#             if row[0] not in region_list:
#                 region_obj = Region.objects.create(name = row[0])
#                 region_list.append(region_obj.name)

#             if row[1] not in country_list:
#                 country_obj = Country.objects.create(name = row[1])
#                 country_list.append(country_obj.name)

#             if row[2] not in item_type_list:
#                 item_type_obj = ItemType.objects.create(type = row[2])
#                 item_type_list.append(item_type_obj.type)
            
#             order_obj = Order(
##                         region = Region.objects.get(name=row[0]),
#                         country = Country.objects.get(name=row[1]),
#                         item_type = ItemType.objects.get(type=row[2]),

#                         sales_channel = row[3],
#                         order_priority = row[4],
                        
#                         order_date = datetime.strptime(row[5], '%m/%d/%Y').date(),
#                         order_id = row[6],
#                         ship_date = datetime.strptime(row[7], '%m/%d/%Y').date(),
                        
#                         units_sold = row[8],
#                         unit_price = row[9],
#                         unit_cost = row[10],
                        
#                         total_revenue = row[11],
#                         total_cost = row[12],
#                         total_profit = row[13],
#                     )
#             print(f"order object created {i} time")
#             i+=1

#             orders_list.append(order_obj)
            
#             if len(orders_list) > 5000:
#                 with Pool() as p:
#                     print("inside with pool")
#                     p.apply_async(Order.objects.bulk_create(orders_list))
                

                # print("inside if multiprocess created")
                # p1.start()
                # p2.start()
                # p3.start()
                # # p4.start()
                # # p5.start()
                
                # p1.join()
                # p2.join()
                # p3.join()
                # p4.join()
                # p5.join()

#                 orders_list=[]
#                 print("list empted")

#         end_time = timezone.now()
#         print(f"time taken is {(end_time - start_time).total_seconds()}")


# def upload(order_list):
#     print("inside upload")
#     Order.objects.bulk_create(order_list)
#     print("after bulk upload")

"""Prototype 2 --> Reopened"""
def run():
    # with transaction.atomic():
    with open('scripts/csv_files/5m_Sales_Records.csv') as file:
        csv_reader = csv.reader(file)
        next(csv_reader) 

        start_time = timezone.now()

        region_list = []
        country_list = []
        item_type_list = []
        orders_list = []

        for row in csv_reader:
            if row[0] not in region_list:
                region_obj = Region.objects.create(name = row[0])
                region_list.append(region_obj.name)

            if row[1] not in country_list:
                country_obj = Country.objects.create(name = row[1])
                country_list.append(country_obj.name)

            if row[2] not in item_type_list:
                item_type_obj = ItemType.objects.create(type = row[2])
                item_type_list.append(item_type_obj.type)
            
            order_obj = Order(
                        region = Region.objects.get(name=row[0]),
                        country = Country.objects.get(name=row[1]),
                        item_type = ItemType.objects.get(type=row[2]),

                        sales_channel = row[3],
                        order_priority = row[4],
                        
                        order_date = datetime.strptime(row[5], '%m/%d/%Y').date(),
                        order_id = row[6],
                        ship_date = datetime.strptime(row[7], '%m/%d/%Y').date(),
                        
                        units_sold = row[8],
                        unit_price = row[9],
                        unit_cost = row[10],
                        
                        total_revenue = row[11],
                        total_cost = row[12],
                        total_profit = row[13],
                    )

            orders_list.append(order_obj)
            
            if len(orders_list) > 5000:
                # p1 = multiprocessing.Process(target=upload, args=(orders_list[:1000], ))
                # p2 = multiprocessing.Process(target=upload, args=(orders_list[1000:2000], ))
                # p3 = multiprocessing.Process(target=upload, args=(orders_list[2000:3000], ))
                # p4 = multiprocessing.Process(target=upload, args=(orders_list[3000:4000], ))
                # p5 = multiprocessing.Process(target=upload, args=(orders_list[4000:], ))
                
                p1 = threading.Thread(target=upload, args=(orders_list[:1000], ))
                p2 = threading.Thread(target=upload, args=(orders_list[1000:2000], ))
                p3 = threading.Thread(target=upload, args=(orders_list[2000:3000], ))
                p4 = threading.Thread(target=upload, args=(orders_list[3000:4000], ))
                p5 = threading.Thread(target=upload, args=(orders_list[4000:], ))

                p1.start()
                p2.start()
                p3.start()
                p4.start()
                p5.start()
                
                p1.join()
                p2.join()
                p3.join()
                p4.join()
                p5.join()

                orders_list=[]

        end_time = timezone.now()
        print(f"time taken is {(end_time - start_time).total_seconds()}")


def upload(order_list):
    Order.objects.bulk_create(order_list)



"""Prototype 8"""
# def run():
#     start_time = timezone.now()
#     with open('scripts/csv_files/5m_Sales_Records.csv', 'r') as file:
#         next(file)
#         reader = pd.read_csv(file, chunksize=5000)

#         for i, chunk in enumerate(reader):
#             start_row = i * 1000
#             end_row = start_row + 1000
            
#             # print(chunk[start_row:end_row])
#             # break
#             first = chunk[start_row:end_row]
#             second = chunk[start_row+1000:end_row+1000]
#             third = chunk[start_row+2000:end_row+2000]
#             fourth = chunk[start_row+3000:end_row+3000]
#             fifth = chunk[start_row+4000:end_row+4000]

#             p1 = multiprocessing.Process(target=upload, args=(first, ))
#             p2 = multiprocessing.Process(target=upload, args=(second, ))
#             p3 = multiprocessing.Process(target=upload, args=(third, ))
#             p4 = multiprocessing.Process(target=upload, args=(fourth, ))
#             p5 = multiprocessing.Process(target=upload, args=(fifth, ))
            
#             # p1 = multiprocessing.Process(target=upload, args=(chunk[:1000]))
#             # p2 = multiprocessing.Process(target=upload, args=(chunk[1001:2000]))
#             # p3 = multiprocessing.Process(target=upload, args=(chunk[2001:3000]))
#             # p4 = multiprocessing.Process(target=upload, args=(chunk[3001:4000]))
#             # p5 = multiprocessing.Process(target=upload, args=(chunk[4001:5000]))
            
#             p1.start()
#             p2.start()
#             p3.start()
#             p4.start()
#             p5.start()
            
#             p1.join()
#             p2.join()
#             p3.join()
#             p4.join()
#             p5.join()
            
#         end_time = timezone.now()
#         print(f"time taken is {(end_time - start_time).total_seconds()}")

# def upload(data):
#     with connections['default'].cursor() as cursor:
#         for _, row in data.iterrows():
#             region_obj, a = Region.objects.get_or_create(
#                                 name = row[0]
#                             )

#             country_obj, b = Country.objects.get_or_create(
#                                 name = row[1]
#                             )

#             item_type_obj, c = ItemType.objects.get_or_create(
#                                 type = row[2]
#                             )

#             order_obj = Order(
#                         ## region = Region.objects.get(name=row[0]),
#                         # country = Country.objects.get(name=row[1]),
#                         # item_type = ItemType.objects.get(type=row[2]),
##                         region = region_obj,
#                         country = country_obj,
#                         item_type = item_type_obj,
                        
#                         sales_channel = row[3],
#                         order_priority = row[4],
                        
#                         order_date = datetime.strptime(row[5], '%m/%d/%Y').date(),
#                         order_id = row[6],
#                         ship_date = datetime.strptime(row[7], '%m/%d/%Y').date(),
                        
#                         units_sold = row[8],
#                         unit_price = row[9],
#                         unit_cost = row[10],
                        
#                         total_revenue = row[11],
#                         total_cost = row[12],
#                         total_profit = row[13],
#                     )
#             # with lock:
#             #     order_obj.save(using='default')
#             order_obj.save()


"""Prototype 7"""
# def run():
#     # with transaction.atomic():
#     start_time = timezone.now()
#     with open('scripts/csv_files/5m_Sales_Records.csv', 'r') as file:
#         next(file)
#         reader = pd.read_csv(file, chunksize=5)
        
#         chunk_list=[]
#         # chunk_list = list(reader)
#         # print(chunk_list)
#         for i, chunk in enumerate(reader):
#             start_row = i * 1000
#             end_row = start_row + 1000
#             chunk_subset = chunk[start_row:end_row]
#             chunk_list.append(chunk_subset)
            
#         print(chunk_list[:1])
        
        # one = next(reader)
        # first = one[:1]
        
        # two = next(reader)
        # second = two[1:2]
        
        # three = next(reader)
        # third = three[2:3]
        
        # four = next(reader)
        # fourth = four[3:4]
        
        # five = next(reader)
        # fifth = five[4:5]

        # print(first)
        # print(second)
        # print(third)
        # print(fourth)
        # print(fifth)
        # multi_processing(reader)
        # print(reader[1,2])
        # print(type(reader))
        # for data in reader:
            # multi_processing(data)
            # print(data)
            # p1 = multiprocessing.Process(target=upload, args=(data,))
            # p1.start()
            # p1.join()
            # break

# def multi_processing(data):
#     print("Inside multiprocessing function")
#     for _, row in data.iterrows():
#         print(row.get_chunk(2))
#         break

# def upload(data):
#     print("inside upload")
#     for row in data.iterrows():
#         print(row)
# def run():
#     start_time = timezone.now()
#     with open('scripts/csv_files/5m_Sales_Records.csv') as file:
#         next(file)
#         reader = pd.read_csv(file)
        
#         data = reader.to_dict()
#         print(data)



"""Prototype 6"""
# lock = Lock
# def run():
#     # with transaction.atomic():
#     start_time = timezone.now()
#     with open('scripts/csv_files/5m_Sales_Records.csv', 'r') as file:
#         next(file)
#         reader = pd.read_csv(file, chunksize=1000)
#         # with Pool(processes=4) as p:
#         with Pool() as p:
#             for data in reader:
#                 p.apply_async(upload, args=(data,))
#                 # p.apply_async(upload, args=(data, lock))
#                 # break
                
#         p.close()
#         p.join()
        
#         end_time = timezone.now()
#         print(f"time taken is {(end_time - start_time).total_seconds()}")
        
# def upload(data):
#     with connections['default'].cursor() as cursor:
#         for _, row in data.iterrows():
#             region_obj, a = Region.objects.get_or_create(
#                                 name = row[0]
#                             )

#             country_obj, b = Country.objects.get_or_create(
#                                 name = row[1]
#                             )

#             item_type_obj, c = ItemType.objects.get_or_create(
#                                 type = row[2]
#                             )

#             order_obj = Order(
#                         ## region = Region.objects.get(name=row[0]),
#                         # country = Country.objects.get(name=row[1]),
#                         # item_type = ItemType.objects.get(type=row[2]),
##                         region = region_obj,
#                         country = country_obj,
#                         item_type = item_type_obj,
                        
#                         sales_channel = row[3],
#                         order_priority = row[4],
                        
#                         order_date = datetime.strptime(row[5], '%m/%d/%Y').date(),
#                         order_id = row[6],
#                         ship_date = datetime.strptime(row[7], '%m/%d/%Y').date(),
                        
#                         units_sold = row[8],
#                         unit_price = row[9],
#                         unit_cost = row[10],
                        
#                         total_revenue = row[11],
#                         total_cost = row[12],
#                         total_profit = row[13],
#                     )
#             # with lock:
#             #     order_obj.save(using='default')
#             order_obj.save(using='default')


"""Prototype 5"""
# output = 'scripts/csv_files/'
# def run():
#     with open('scripts/csv_files/5m_Sales_Records.csv', 'r') as file:
#         next(file)
#         for i, row in enumerate(pd.read_csv(file, chunksize=4)):
#             temp_file = io.StringIO()
#             row.to_csv(temp_file, index=False)
#             temp_file.seek(0)
#             chunk_threading(temp_file)
#             temp_file.close()
#             break


# def chunk_threading(temp_file):
#     print(temp_file.getvalue())
#     temp_list_1 = []
#     temp_list_2 = []
#     temp_list_3 = []
#     temp_list_4 = []
#     temp_list_5 = []

#     temp_file = list(temp_file)
#     print(len(temp_file))
#     n=1
#     for i in range(0,5):
#         if len(temp_file) == 5:
#             temp_list_1 = temp_file[n:]
#         elif len(temp_file) == 4:
#             temp_list_2 = temp_file[n:]
#         # elif len(temp_file) == 3:
#         #     temp_list_3 = temp_list
#         # elif len(temp_file) == 2:
#         #     temp_list_4 = temp_list
#         # elif len(temp_file) == 1:
#         #     temp_list_5 = temp_list
            
#     print(temp_list_1)
#     print(temp_list_2)
        # print(temp_list_3)
        # print(temp_list_4)
        # print(temp_list_5)
        # print("ye", i)
    # for i in range(0,4):
    # data = temp_file.readlines()
    # print(data)
    # print(temp_file.getvalue())
    # data = pd.read_csv(temp_file)
    # print(data)
    # # num_chunks = int(len(data) / 1)
    # for i, row in enumerate(data):
    #     print(i, row)
    
    # split the data into chunks of size 100 and save each chunk to a separate file
    # for i, chunk in enumerate(pd.array_split(data, num_chunks)):
    #     chunk.to_csv(f'chunk_{i}.csv', index=False)
    # for i, row in enumerate(temp_file):
    #     print(i, row)
    # t1 = threading.Thread(target=upload, args=chunk)
    # t2 = threading.Thread(target=upload, args=chunk)
    # t3 = threading.Thread(target=upload, args=chunk)
    # t4 = threading.Thread(target=upload, args=chunk)
    # t5 = threading.Thread(target=upload, args=chunk)
    
    # t1.start()
    # t2.start()
    # t3.start()
    # t4.start()
    # t5.start()
    
    # t1.join()
    # t2.join()
    # t3.join()
    # t4.join()
    # t5.join()

# def upload(temp_file):
#     pass
    # print("HEHE, we got the file")
    # print(temp_file)


            # output_files = os.path.join(output, f"output_{i}")
            # chunk = row.to_csv(output_files, index=False)
            # chunk_threading(output_files)
            # print(row.to_string(index=False))
            # break

#"""Prototype 4"""
# def run():
#     t1 = threading.Thread(target=upload)
#     t2 = threading.Thread(target=upload)
#     t3 = threading.Thread(target=upload)
#     t4 = threading.Thread(target=upload)
#     t5 = threading.Thread(target=upload)
    
#     t1.start()
#     t2.start()
#     t3.start()
#     t4.start()
#     t5.start()
    
#     t1.join()
#     t2.join()
#     t3.join()
#     t4.join()
#     t5.join()
# def upload():
#     with transaction.atomic():
#         with open('scripts/csv_files/5m_Sales_Records.csv', 'r') as file:
#             csv_reader = csv.reader(file)
#             next(csv_reader)
        
#             for row in csv_reader:
#                 region_list = []
#                 country_list = []
#                 item_type_list = []
#                 # orders_list = []

#                 if row[0] not in region_list:
#                     region_obj = Region.objects.create(name = row[0])
#                     region_list.append(region_obj.name)

#                 if row[1] not in country_list:
#                     country_obj = Country.objects.create(name = row[1])
#                     country_list.append(country_obj.name)

#                 if row[2] not in item_type_list:
#                     item_type_obj = ItemType.objects.create(type = row[2])
#                     item_type_list.append(item_type_obj.type)
                
#                 order_obj = Order(
##                     region = Region.objects.get(name=row[0]),
#                     country = Country.objects.get(name=row[1]),
#                     item_type = ItemType.objects.get(type=row[2]),

#                     sales_channel = row[3],
#                     order_priority = row[4],
                    
#                     order_date = datetime.strptime(row[5], '%m/%d/%Y').date(),
#                     order_id = row[6],
#                     ship_date = datetime.strptime(row[7], '%m/%d/%Y').date(),
                    
#                     units_sold = row[8],
#                     unit_price = row[9],
#                     unit_cost = row[10],
                    
#                     total_revenue = row[11],
#                     total_cost = row[12],
#                     total_profit = row[13],
#                 )
                
#                 order_obj.save()


"""Prototype 3"""
# def run():
#     with transaction.atomic():
#         with open('scripts/csv_files/5m_Sales_Records.csv') as file:
#             csv_reader = csv.reader(file)
#             next(csv_reader)                #Skip first row of csv file --> It contains headers

#             start_time = timezone.now()
#             region_list = []
#             country_list = []
#             item_type_list = []
#             orders_list = []
#             i=1
#             for row in csv_reader:
#                 if row[0] not in region_list:
#                     region_obj = Region.objects.create(name = row[0])
#                     region_list.append(region_obj.name)

#                 if row[1] not in country_list:
#                     country_obj = Country.objects.create(name = row[1])
#                     country_list.append(country_obj.name)

#                 if row[2] not in item_type_list:
#                     item_type_obj = ItemType.objects.create(type = row[2])
#                     item_type_list.append(item_type_obj.type)
                
#                 # base_obj = BaseModel()
#                 # base_obj.save()
#                 order_obj = Order(
#                             # basemodel_ptr_id = base_obj.uuid,
##                             region = Region.objects.get(name=row[0]),
#                             country = Country.objects.get(name=row[1]),
#                             item_type = ItemType.objects.get(type=row[2]),

#                             sales_channel = row[3],
#                             order_priority = row[4],
                            
#                             order_date = datetime.strptime(row[5], '%m/%d/%Y').date(),
#                             order_id = row[6],
#                             ship_date = datetime.strptime(row[7], '%m/%d/%Y').date(),
                            
#                             units_sold = row[8],
#                             unit_price = row[9],
#                             unit_cost = row[10],
                            
#                             total_revenue = row[11],
#                             total_cost = row[12],
#                             total_profit = row[13],
#                         )

#                 orders_list.append(order_obj)
#                 if len(orders_list) > 1000:
#                     # print(orders_list[1].uuid)
#                     # bs_mdls = BaseModel.objects.bulk_create()
#                     # print("bs_mdls created")
#                     for i in orders_list:
#                         print(i)
#                         Order.objects.create(**i)  #, basemodel_ptr_id=[bs_mdl.uuid for bs_mdl in bs_mdls]
#                         orders_list = []

#                 print(f"Entry = {i} created.")
#                 i+=1
#             end_time = timezone.now()
#             print(f"time taken is {(end_time - start_time).total_seconds()}")



"""Prototype 2"""
# def run():
#     # with transaction.atomic():
#     with open('scripts/csv_files/5m_Sales_Records.csv') as file:
#         csv_reader = csv.reader(file)
#         next(csv_reader)                #Skip first row of csv file --> It contains headers

#         start_time = timezone.now()
#         region_list = []
#         country_list = []
#         item_type_list = []
#         orders_list = []
#         i=1
#         for row in csv_reader:
#             if row[0] not in region_list:
#                 region_obj = Region.objects.create(name = row[0])
#                 region_list.append(region_obj.name)

#             if row[1] not in country_list:
#                 country_obj = Country.objects.create(name = row[1])
#                 country_list.append(country_obj.name)

#             if row[2] not in item_type_list:
#                 item_type_obj = ItemType.objects.create(type = row[2])
#                 item_type_list.append(item_type_obj.type)
            
#             order_obj = Order(
##                         region = Region.objects.get(name=row[0]),
#                         country = Country.objects.get(name=row[1]),
#                         item_type = ItemType.objects.get(type=row[2]),

#                         sales_channel = row[3],
#                         order_priority = row[4],
                        
#                         order_date = datetime.strptime(row[5], '%m/%d/%Y').date(),
#                         order_id = row[6],
#                         ship_date = datetime.strptime(row[7], '%m/%d/%Y').date(),
                        
#                         units_sold = row[8],
#                         unit_price = row[9],
#                         unit_cost = row[10],
                        
#                         total_revenue = row[11],
#                         total_cost = row[12],
#                         total_profit = row[13],
#                     )

#             # order_obj.save()
#             # print(f"objects - {i} created.")
#             # i+=1

#             orders_list.append(order_obj)
            
#             if len(orders_list) > 50000:
#                 print(f"chunk {i} of 50,000 data entries.")
#                 Order.objects.bulk_create(orders_list)
#                 orders_list=[]
#                 i+=1
#         end_time = timezone.now()
#         print(f"time taken is {(end_time - start_time).total_seconds()}")


"""Prototype 1"""
# def run():
#     with transaction.atomic():
#         with open('scripts/csv_files/5m_Sales_Records.csv') as file:
#             csv_reader = csv.reader(file)
#             next(csv_reader)                #Skip first row of csv file --> It contains headers

#             for row in itertools.islice(csv_reader, BATCH_SIZE):
#                 region_dataset, a = Region.objects.get_or_create(
#                                     name = row[0]
#                                 )

#                 country_dataset, b = Country.objects.get_or_create(
#                                     name = row[1]
#                                 )

#                 item_type_dataset, c = ItemType.objects.get_or_create(
#                                     type = row[2]
#                                 )
                
#                 order_dataset = Order(# region = region_dataset,
#                             country = country_dataset,
#                             item_type = item_type_dataset,

#                             sales_channel = row[3],
#                             order_priority = row[4],
                            
#                             order_date = datetime.strptime(row[5], '%m/%d/%Y').date(),
#                             order_id = row[6],
#                             ship_date = datetime.strptime(row[7], '%m/%d/%Y').date(),
                            
#                             units_sold = row[8],
#                             unit_price = row[9],
#                             unit_cost = row[10],
                            
#                             total_revenue = row[11],
#                             total_cost = row[12],
#                             total_profit = row[13],
#                         )

#                 order_dataset.save()
