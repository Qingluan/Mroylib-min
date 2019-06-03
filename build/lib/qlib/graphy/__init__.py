import random

def random_choice(lst, num=None):
	if not num:
		num = len(lst)
	ix = random.randint(0, num-1)
	return lst[ix]