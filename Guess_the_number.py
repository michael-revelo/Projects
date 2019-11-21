import random
import sys 

random_number = random.randint(1, 10)
guesses = []

def guess_again(guess):

	if int(guess) not in range(1, 11):
		print("Guess is out of bounds. Try again.")

	elif int(guess) < random_number:
		print("Your guess was " + str(guess) + ". " + "Sorry, your guess is too low... ")

	elif int(guess) > random_number:
		print("Your guess was " + str(guess) + ". " + "Sorry, your guess is too high... ")

	else:
		exit("Congratulations! YOU WIN.")
		

	guesses.append(guess)
	print("Your guesses are: " + ", ".join(guesses))
	try_again = raw_input("Try again ")
	guess_again(try_again)

def main():

	start_game = raw_input("Guess a number between 1 and 10. ")

	if int(start_game) == random_number:
		print("Congratulations! YOU WIN.")

	else:
		guess_again(start_game)	

main()



