#Write a Pizza Delivery Program
# This program takes a pizza order from a customer, including size and toppings, and calculates the total cost.

print("Welcome to Python Pizza Deliveries!")
size = input("What size pizza do you want? S, M, or L: ").upper()
Pepperoni = input("do you want? Pepperoni on your Pizza? Y or N ").upper()
extra_cheese = input("Do you want extra cheese? Y or N: ").upper()

#Small Pizza = $15
#Medium Pizza = $20
#Large Pizza = $25

#Pepperoni for Small Pizza = +$2
#Pepperoni for Medium or Large Pizza = +$3

#adding Extra cheese for any size pizza = +$1

#Todo : work out how much they need to pay on their size choice



bill = 0

if size == "S":
    bill += 15
elif size == "M":
    bill += 20
elif size == "L":
    bill += 25
else:
    print("Invalid size selected.")
    exit()


#Todo : work out how much they need to pay on their topping choice

if Pepperoni == "Y":
    if size == "S":
        bill = bill + 2
    else:
        bill += 3


#Todo : work out how much they need to pay if they want extra cheese

if extra_cheese == "Y":
    bill += 1

print(f"Your final bill is: ${bill}.")