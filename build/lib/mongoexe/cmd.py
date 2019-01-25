# -*- coding: utf-8 -*-
from argparse import ArgumentParser
from mongoexe.mon import Mon

def main():
	par = ArgumentParser()
	par.add_argument("file", nargs="+",help="load file ..")
	par.add_argument("-t", "--type", default='xlsx', help="set file tp, default: xlsx")
	par.add_argument("-a", "--args", nargs="*", default=['local'],help="set file tp, default: xlsx")
	par.add_argument("-k", "--keys", nargs="+" ,help="set key to insert ..")
	

	args = par.parse_args()

	mon = Mon()
	db_name = None
	col_name = None
	if len(args.args) == 1:
		db_name = args.args[0]
	elif len(args.args) == 2:
		db_name = args.args[0]
		col_name = args.args[1]

	for f in args.file:
		mon.from_xlsx(f, list(args.keys), db_na=db_name, col_na=col_name)

