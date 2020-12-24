import sys

p1 = sys.argv[1]
p2 = sys.argv[2]
print("arg1 text:" + p1)
print("arg2 select:" + p2)

# a file upload parameter
with open(sys.argv[3], 'r') as f:
    print("arg3 first line from file:")
    print(f.readline())

# a multi-select parameter
print("arg 4 mult-select:")
for i, part in enumerate(sys.argv[4].split(",")):
    print(i)
    print(part)

print("arg 5 date:" + sys.argv[5])
print("arg 6 checkbox:" + sys.argv[6])