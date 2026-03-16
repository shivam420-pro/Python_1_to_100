def add(one,two):
    return one+two

def sub(one,two):
    return one-two

def mul(one,two):
    return one*two

def div(one,two):
    return one/two

Operation = {
    "+":add,
    "-":sub,
    "*":mul,
    "/":div
}
def Calculator():
    should_continue = True

    n1 = float(input("What is the first number : "))

    while should_continue:

        for symbol in Operation:
            print(symbol)

        operation_symbol = input("pick the operation thst you want to perform :")

        n2 = float(input("What is the next number : "))

        answer = Operation[operation_symbol](n1,n2)
        print(f"{n1} {operation_symbol} {n2} = {answer}")

        choose = input(f'Type "y" to continue calculating with the {answer}, or type "n" to start a new calculation.')

        if choose == "y":
            n1 = answer
        else:
            should_continue = False
            print("Good bye")
            print("\n"*20)
            Calculator()    
Calculator()