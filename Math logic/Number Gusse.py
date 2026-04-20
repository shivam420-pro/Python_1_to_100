import random

Easy_level = 10
Hard_level = 5


def check_answer(guess_num, actual_number, turn):
    if guess_num > actual_number:
        print("Too high.")
        return turn - 1

    elif guess_num < actual_number:
        print("Too low.")   
        return turn - 1

    else:
        print("Congratulations! You guessed the number correctly.")


#set the defult number of attempts to 10

def set_difficulty():
    level = input("Choose a difficulty level (easy, hard): ")
    if level == "easy":
        return Easy_level
    else:
        return Hard_level



def game():

    #choosing number from 1 to 100 
    print("Welcome to the Number Guessing Game!")
    print("I will think of a number between 1 and 100, and you have to guess it.")
    Random_number = random.randint(1, 100)

    turn = set_difficulty()
    

    #Let user guess the number

    #track the number of attempt and reduce it by 1 for each attempt
    #Repeat the process until the user guesses the number or runs out of attempts
    guess = 0
    while guess != Random_number:
        print(f"You have {turn} attempts to guess the number.")
        guess = int(input("Enter your guessing number: "))
        turn = check_answer(guess,Random_number,turn)
        if turn == 0:
            print("You've run out of attempts. You lose.")
            return


game()