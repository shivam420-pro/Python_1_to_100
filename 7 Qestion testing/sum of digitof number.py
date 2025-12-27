#Find the sum of digits of a number

num  = int(input("Enter the number to give the sum of your digit :- "))

sum_digits = 0

while num > 0:
    digit = num % 10
    sum_digits = sum_digits + digit
    num = num // 10

print("Sum of digits is:", sum_digits)