import random

player = ''

def possible(player, computer):

	if player == 'Rock':
		if computer == 'Paper':
			print("Computer Wins")

		else:
			print("Player Wins")

	elif player == 'Paper':
		if computer == 'Scissors':
			print("Computer Wins")

		else:
			print("Player Wins")

	elif player == 'Scissors':
		if computer == 'Rock':
			print("Computer Wins")

		else:
			print("Player Wins")

	return play_again()

def play_again():

	again = raw_input("Would you like to keep playing? ")

	if again[0].lower() == 'y':
		main()

	elif again[0].lower() == 'n':
		print("Thanks for playing")

	else:
		play_again()

def main():

	player = raw_input("Rock, Paper, or Scissors? ")
	computer = ['Rock', 'Paper', 'Scissors']

	if player[0].lower() == 'r':
		player = 'Rock'

	if player[0].lower() == 'p':
		player = 'Paper'

	if player[0].lower() == 's':
		player = 'Scissors'

	computer = random.choice(computer)
	print(str(player) + ' ' + "vs " + str(computer))

	if player == computer:
		print("Draw")
		play_again()

	else:
		possible(player, computer)

main()