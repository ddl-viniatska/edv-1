import csv

with open('/mnt/diabetes_read_only/diabetes.csv', 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        print(row)
