# A and B when A and B are both True
# A or B when either A or B is True
# not A when A is False


# Nested if / else and elif
# EX : First check with hight is greater then 120 cm then check age is greater than 18 to allow ride with a friend otherwise alone
# now more advance proble statment is if 
    # age 12 less than 5$
    # age 12 to 18 then 7$
    # age above 18 then 12$
# now we have to consider the photo becaue in ride if rider want picture in that ride time then is have to pay 3$ more
# Extenstion of that is if person is between 45 to 55 age then it ride price is 0$


height = float(input("Enter your height : "))
bill=0
if height >= 120 :
    print("You are Eligible to ride the Rollercoaster !!!!")
    age_inpute = int(input("Enter your age : "))

    if age_inpute < 18.5 :
        bill = 5
        print(" Your ticket price is $5")
    elif age_inpute <= 25 :
        bill = 7
        print(" Your ticket price is $7")
    elif age_inpute >=45 and age_inpute <=55:
        bill = 0
        print(" Your ticket price is $0")
    else :
        bill = 12
        print(" Your ticket price is $12")
    
    Ride_Photo = input("Do need photo in ride ? Type Y or N : ")
    if Ride_Photo == "Y":
        bill += 3
        print(f" Your total bill is ${bill} ")
    else :
        print(f" Your total bill is ${bill} ")
else :
    print(f" Sorry You are not Eligible to ride the Rollercoaster, your hight is {height}cm !!!!")