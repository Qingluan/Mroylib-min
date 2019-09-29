from ._pipe import get_output, stdout
import argparse
__call__ = [
	'get_output',
	'stdout',
	'input_default',
]


def input_default(prompt, default=''):
	res = input(prompt)
	if not res:
		return default
	return res


def GeneratorApi(kargs, doc="Usage help"):
    import argparse
    """
    generator a cmd shell
    
     this function will generate a api.
            will use 'parser.add_argument()'
            examp: {
                'url' :  "set a target url"
                    ...
            }
            the 'url' will be parse to :
                parser.add_argument("--url", default=None, help="set a target url")
            ...
            
            examp:
            if key's all case is upcase. will gen a positional arg.
            {
                'URL': 'set a target url',
                ...
            }
            -->
                parser.add_argument("url", default=None, help="set a target url")

            examp: 
            if key startswith upcase. will be follow
            {
                'Url' :  "set a target url"
                    ...
            }
             -->
                parser.add_argument("-u", "--Url", default=None, help="set a target url")

            examp: 
            if value is boolean.
            {
                'ed' :  (False,  "end some ?")
                    ...
            }
            

            --> 
                parser.add_argument("--ed", default=False, help="end some?")
    """
    upcase = set([ i for i in 'QWERTYUIOPASDFGHJKLZXCVBNM'])
    parser = argparse.ArgumentParser(usage='Usage', description=doc)
    # parser.add_argument("-T", "--thread", default=1, type=int, help="add some new data to module's DB")
    # parser.add_argument("--Editor", default=False, action='store_true', help="edit this module")
    # parser.add_argument("-ad", "--add-data", default=None, help="add some new data to module's DB")
    # parser.add_argument("-dd", "--delete-data", default=False, action='store_true', help="delte some data to module's DB")
    
    for key in kargs:
        key_set = set([i for i in key])
        val = kargs[key]
        pk = []
        pko = {}
    
        if key.endswith('*'):
            pko['nargs'] = '*'
            key = key[:-1]
    
        # key area
        if  (key_set & upcase) == key_set:
            pk.append(key.lower())
        elif key[0] in key_set:
            pk += ['-%s' % key[0].lower() , '--%s' % key]
        else:
            pk.append(key)
    
        # val area
        if isinstance(val, str):
            pko['default'] = None
            pko['help'] = val
        elif isinstance(val, tuple):
            pko['default'] = val[0] if isinstance(val[0], bool) else v[1]
            pko['action'] = ('store_%s' % True if not pko['default'] else 'store_%s' % False).lower()
            pko['help'] = val[1] if isinstance(val[1], str) else v[0]
    
        else:
            LogControl.err(key, val)
            continue
        parser.add_argument(*pk, **pko)
    
    return parser.parse_args()
