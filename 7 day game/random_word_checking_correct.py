import random

# List of words
words = ['shivam', 'sharma','sangam','sweta','bharath','urmila']

# Choose a random word
secret_word = random.choice(words)
print(secret_word)

# Variables
guessed_letters = []
attempts = 6

print("🎮 Welcome to Hangman Game!")
print("You have", attempts, "attempts")

# Game loop
while attempts > 0:
    display_word = ""
    
    # Show word with guessed letters
    for letter in secret_word:
        if letter in guessed_letters:
            display_word += letter + " "
        else:
            display_word += "_ "
    
    print("\nWord:", display_word)

    # Check win condition
    if "_" not in display_word:
        print("🎉 Congratulations! You guessed the word:", secret_word)
        break

    # Take user input
    guess = input("Guess a letter: ").lower()

    # Validation
    if guess in guessed_letters:
        print("⚠️ You already guessed that letter!")
        continue

    guessed_letters.append(guess)

    # Wrong guess
    if guess not in secret_word:
        attempts -= 1
        print("❌ Wrong guess! Attempts left:", attempts)

# Game over
if attempts == 0:
    print("\n💀 Game Over! The word was:", secret_word)
