#fizzbuzz game creation 
#ruls
#Your program should print each number from 1 to 100 in turn and include number 100.
#But when the number is divisible by 3 then instead of printing the number it should print "Fizz".
#When the number is divisible by 5, then instead of printing the number it should print "Buzz".`
#And if the number is divisible by both 3 and 5 e.g. 15 then instead of the number it should print "FizzBuzz"



fizzbuzz_print = 0

for fizzbuzz_print in range(1,101):
    if fizzbuzz_print%3 == 0 and fizzbuzz_print%5 == 0:
        fizzbuzz_print = print("FizzBuzz")
    elif fizzbuzz_print%3 == 0:
        fizzbuzz_print =print("Fizz")
    elif fizzbuzz_print%5 == 0:
        fizzbuzz_print = print("Buzz")
    else:
        print(fizzbuzz_print)