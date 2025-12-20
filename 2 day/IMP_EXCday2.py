print("Welcome to the tip calculator!")
Bill = input("What was the total bill? $ :")
Tip_from_all = input("How much tip would you like to give? 10, 12, or 15? :")
Spliting_peopel = input("How many people to split the bill? : ")

Total_Bill = float(Bill) + float(Tip_from_all)
Spit_bill = Total_Bill / int(Spliting_peopel)

print(f"Each person Should Pay : ${round(Spit_bill,2)}")
print("Thank you for using tip calculator")