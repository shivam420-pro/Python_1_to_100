#list also called data structure in python
#list is ordered, changeable, and allows duplicate values
#list is written with square brackets []
#list indexing starts from 0


from random import random


my_first_list = ["apple", "banana", "cherry"]
print(my_first_list)
print(my_first_list[0])  # Output: apple

#list with different data type
my_second_list = ["hello", 42, 3.14, True]
print(my_second_list)

#nested list
my_nested_list = [1, 2, [3, 4], 5]
print(my_nested_list)
print(my_nested_list[2])  # Output: [3, 4]
print(my_nested_list[2][0])  # Output: 3

#list manipulation
fruits = ["apple", "banana", "cherry"] 
fruits.append("orange")  # Add an item to the end of the list
print(fruits)

fruits.remove("banana")  # Remove an item from the list
print(fruits)

fruits[1] = "blueberry"  # Change the value of an item in the list
print(fruits)

#list length
print(len(fruits))  # Output: 3


my_frends = ["Alice", "Bob", "Charlie","shivam","rahul","deepak"]
random_index = random.randint(0,len(my_frends)-1)
print(f"{my_frends[random_index]} is selected to pay the bill")
