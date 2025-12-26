
# Number & Math Functions
abs(-10)        # 10
round(3.6)      # 4
pow(2, 3)       # 8
max(3, 7, 1)    # 7
min(3, 7, 1)    # 1
sum([1, 2, 3])  # 6


#Type Conversion Functions
int("10")       # 10
float("3.5")    # 3.5
str(100)        # "100"
bool(0)         # False
list("abc")     # ['a','b','c']
tuple([1,2,3])  # (1,2,3)
set([1,2,2])    # {1,2}

#Input / Output Functions
print("Hello")
input("Enter name: ")

#Length & Iteration
len("Python")     # 6
range(5)          # 0 1 2 3 4
enumerate(['a','b'])

#Check & Compare
type(10)          # <class 'int'>
isinstance(10,int)
id(10)


#Logic & Conditions
all([True, True])
any([False, True])

# Sorting & Reversing
sorted([3,1,2])     # [1,2,3]
reversed([1,2,3])

# Character & ASCII
ord('A')    # 65
chr(65)     # 'A'


#Help & Info
help(print)
dir(str)


#Most Important for Beginners (Must Learn First)
print()
input()
len()
type()
int()
float()
str()
range()
list()
dict()
set()


# Mini Practice Example
name = input("Enter name: ")
print("Length:", len(name))
print("Upper:", name.upper())
