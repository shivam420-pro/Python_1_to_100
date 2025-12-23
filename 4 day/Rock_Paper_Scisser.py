# Rock Paper Scissors ASCII Art
import random
import os
os.system('cls' if os.name == 'nt' else 'clear')

# Rock
rock = '''
    _______
---'   ____)
      (_____)
      (_____)
      (____)
---.__(___)
'''''

# Paper
paper = '''
     _______
---'    ____)____
           ______)
          _______)
         _______)
---.__________)
'''

# Scissors
scissors = '''
    _______
---'   ____)____
          ______)
       __________)
      (____)
---.__(___)
'''


# Scissors
scissors = '''
    _______
---'   ____)____
          ______)
       __________)
      (____)
---.__(___)
'''


my_designs = [rock, paper, scissors]


you_choose = int(input("What do you choose? Type 0 for Rock, 1 for Paper or 2 for Scissors.\n"))
if you_choose >=0 and you_choose <=2:
    print(my_designs[you_choose])

computer_choose = random.randint(0,2)
print("Computer chose:")
print(my_designs[computer_choose])


print("You chose:",you_choose)
print("Computer chose:",computer_choose)

if you_choose < 0 or you_choose > 2:
    print("Invalid input! You lose!")
elif you_choose == 0 and computer_choose == 2:
    print("You win!")
elif computer_choose == 0 and you_choose == 2:
    print("You lose!")
elif computer_choose > you_choose:
    print("You lose!")
elif you_choose > computer_choose:
    print("You win!")
elif you_choose == computer_choose:
    print("It's a draw!")

