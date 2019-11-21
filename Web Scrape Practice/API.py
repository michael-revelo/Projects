import requests
import random
from bs4 import BeautifulSoup
from tags import all_tags

def beautifier():

	website = input("Please provide a URL to be beautified: ")

	pretty = BeautifulSoup(requests.get(str(website)).text)
	print(pretty.prettify())

	question = input('What tag would you like to see? An example could be ' + str(random.choice(all_tags)) + ' ')

	if question in all_tags:
		for i in '<>':
			question = question.replace(i, "")

		all_or_one = input('Would you like to see one example or all? ')
		
		if all_or_one == 'all':
			get_all = 'pretty' + '.' + 'find_all' + '(' + '"' + question + '"' + ')' 
			print(get_all)
			print(eval(get_all))

		else:
			get_single = 'pretty' + '.' + question
			print(eval(get_single))
		
	else:
		beautifier()

beautifier()

### if the tag does not exist in the list, call on 'question' not 'beautifier' all over again
### add elif for getting links
### limited by tags for now - user cannot seacrh for a word, etc. ; can start with tags and filter down - problem: a word may be across multiple tags