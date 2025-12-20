#Add some if/elif/else statements to the BMI calculator so that it interprets the BMI values calculated.
#If the bmi is under 18.5 (not including), print out "underweight"
#If the bmi is between 18.5 (including) and 25 (not including), print out "normal weight"
#If the bmi is 25 (including) or over, print out "overweight"

height = float(input("enter your height in m: "))
weight = float(input("enter your weight in kg: "))

bmi = height/weight

if bmi < 18.5:
    print(f"Your BMI is {bmi}, you are underweight.")
elif bmi < 25:
    print(f"Your BMI is {bmi}, you have a normal weight.")
else:
    print(f"Your BMI is {bmi}, you are overweight.")    