def compressor(string):

    sorted_string = ''.join(sorted(set(string)))
	
    print(''.join([str(sorted_string)[i] + str(string.count(sorted_string[i])) for i in range(len(sorted_string))]))


