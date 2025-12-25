#Nested if / else
# EX : First check with hight is greater then 120 cm then check age is greater than 18 to allow ride with a friend otherwise alone
print(" welcome to Rollercoaster Ride in Jocker Amusement Park")
hight = float(input("Enter your Hight in cm :"))


if hight >= 120:
    age = int(input("Enter your Age :"))
    if age >= 18:
        print(" You are Eligible to ride the Rollercoaster with a Friend !!!!")
    else:
        print(f" Sorry You are not Eligible to ride the Rollercoaster, your age is {age}!!!!")
else:
    print(f" Sorry You are not Eligible to ride the Rollercoaster, your hight is {hight}cm !!!!")