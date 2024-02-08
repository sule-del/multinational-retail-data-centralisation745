import random
word_list = "grapes", "apples", "oranges", "mangos", "kiwi"
print(word_list)
random_fruit = random.choice(word_list)
word = random.choice(word_list)
print("Randomly selected fruit:", word)

guess = input("Enter a single letter: ")
print("You entered:", guess)

guess = input("Enter a single letter: ")
if len(guess) == 1 and guess.isalpha():
    print("Good guess!")
else:
    print("Oops! That is not a valid input.")
