#Swap two variables
first = input("Enter your first variable ")
second = input("Enter your second variable ")

print(f"Before swapping first variable is {first} and second variable is {second}")
third = first
first = second
second = third

print(f"After swapping first variable is {first} and second variable is {second}")