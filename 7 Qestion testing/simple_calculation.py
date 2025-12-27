# Take two numbers and an operator (+ - * /) and perform the operation.

print("To Perform calculation between 2 digit number " \
"Enter number  :- ")

first_digit = int(input("Enter First Digit :-"))
second_digit = int(input("Enter Second Digit :-"))

operation =True

while operation:
    operation = str(input("enter the operation which you want +, -, /, * :- \n"))
    if operation =="exit":
        exit()
    elif operation == "+":
        print(f"Sum of your {first_digit} and {second_digit} = is ", first_digit+second_digit)
    elif operation == "-":
        print(f"Subtraction of your {first_digit} and {second_digit} = is ", first_digit-second_digit)
    elif operation == "/":
        print(f"Division of your {first_digit} and {second_digit} = is ", first_digit/second_digit)
    elif operation == "*":
        print(f"Multiplication of your {first_digit} and {second_digit} = is ", first_digit*second_digit)
    else:
        print("You have enter in correct Operation :--!!!\n \n \n")
        