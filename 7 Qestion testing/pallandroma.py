#Check if a number is a palindrome
#Example:  121 → 121

num = int(input("Enter the number to reverse the digit :- "))
number = num

reverse = 0
while num > 0:
    digit = num % 10   #take the last digit from the digit   
    reverse = reverse * 10 + digit
    num = num // 10
print("Reversed number is:", reverse)
if reverse == number:
    print(f"your {number} is palindrome")
else:
    print(f"your {number} is not palindrome")