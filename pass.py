import random

chars = 'abcdefghijklmnopqrstuvwxyz'
nums = '123456789'
caps = chars.upper()
symbols = '~!@#$%^&*-_=+<>?/;:'
all_chars = chars + nums + caps + symbols

length = int(input("Password length? "))

password = ''

for i in range(length):
	password += random.choice(all_chars)
print(password)