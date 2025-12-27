# Write a program to calculate the factorial of a number.

user_input = int(input("Enter the value to find the factorial number :- "))

number = 1 

if user_input < 0:
    print("Enter the number greater then 0 ")
else:
    for num in range(1,user_input+1):
        number = number*num
    print(f"factorial number of {user_input} is : {number}")
number_in_string = str(number)
print("length of Digit is  : ",len(number_in_string))