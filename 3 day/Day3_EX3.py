# Nested if / else and elif
# EX : First check with hight is greater then 120 cm then check age is greater than 18 to allow ride with a friend otherwise alone
# now more advance proble statment is if 
    # age 12 less than 5$
    # age 12 to 18 then 7$
    # age above 18 then 12$

print(" welcome to Rollercoaster Ride in Jocker Amusement Park")
hight = float(input("Enter your Hight in cm :"))


if hight >= 120:
    print("You are Eligible to ride the Rollercoaster !!!!")
    age = int(input("Enter your Age :"))
    if age < 12:
        print(" Your ticket price is $5")
    elif age <= 18:
        print(" Your ticket price is $7")
    else:
        print(" Your ticket price is $12")
else:
    print(f" Sorry You are not Eligible to ride the Rollercoaster, your hight is {hight}cm !!!!")   