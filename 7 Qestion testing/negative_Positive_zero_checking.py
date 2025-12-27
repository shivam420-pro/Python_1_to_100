# Take a number from the user and check if it is positive, negative, or zero.

User_input = int(input("Enter the muber to check wheter the vaule is Negative, Positive and Zero :- \n"))

if User_input == 0:
    print(f"You have enter {User_input} is Zero.")
elif User_input > 0:
    print(f"You have enter {User_input} is Positive.")
else:
    print(f"You have enter {User_input} is Negative.")