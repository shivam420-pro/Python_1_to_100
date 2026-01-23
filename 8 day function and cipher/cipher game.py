alphabet_lower = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']


direction = input("Type 'encode' to encrypt, type 'decode' to decrypt:  \n").lower
test = input("Type your message: \n").lower()
shift = int(input("Type the shift number :\n"))

#To-Do 1: Create a function called 'encrypt()' that take ' original_text ' and 'shift_amount' as 2 inputs.

def encrypt(original_text,shift_amount):
    for letter in original_text:
        position = alphabet_lower.index(letter)
        new_position = position + shift_amount
        new_letter = alphabet_lower[new_position]
        print(new_letter)

#To-Do 2: Inside the encrypt() function, shift each letter of the 'original_text' forward in the alphabet by the shift amount and print the encrypted text

def encrypt(original_text,shift_amount):
    encrypted_text = ""
    for letter in original_text:
        position = alphabet_lower.index(letter)
        new_position = position + shift_amount
        if new_position >= len(alphabet_lower):
            new_position = new_position - len(alphabet_lower)
        new_letter = alphabet_lower[new_position]
        encrypted_text += new_letter
    print(f"The encrypted text is: {encrypted_text}")

#To-Do 4: What happens if you try to shift z forward by 9? can you fix the code?



#To-Do 3: call the encrypt() function and pass in the user inputs. you should be able to test the code and encrypt a message.
