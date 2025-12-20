#Modulo Operator
#The modulo operator % gives the remainder after division of one number by another.
print(10 % 3)  # Output: 1 because 10 divided by 3 is 3 with a remainder of 1

#Check if a number is even or odd

Number_input = int(input("Enter a number to check if it is even or odd: "))
if Number_input % 2 == 0:
    print(f"The number {Number_input} is Even.")
else:
    print(f"The number {Number_input} is Odd.")