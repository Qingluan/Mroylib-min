import inspect
import re
# import torch.nn as NN
from collections import OrderedDict
from termcolor import colored, cprint
import tabulate
import itertools
try:
    import requests
except ImportError:
    pass

INIT_ATT_EXE = re.compile(r'(self\.\w+)\s*?=\s*?([\w\.]+)')
IF_STATE = re.compile(r'(if [\w\s]+)')
FOR_STATE = re.compile(r'for\s+?.+\s+?in\s+?([\w\.]+)')
ST_SPANCE = re.compile(r'^\s+')
ATT = re.compile(r'([\w\.]+)\s*?=\s*?(.+)')
INPUT_VARS = re.compile(r'\((.+)\)')
WORD = re.compile(r"(\w+)")
ATTRS_NAME =re.compile(r"([\w\.]+)")
OUTPUT = re.compile(r'return\s(.+)')
IF_FUNC = re.compile(r'([\w\.]+)')
# FUNC = re.compile(r'(\w+)')
FUNC = re.compile(r'((?:self\.)?\w+)')
OPERATOR = re.compile(r'((?:self\.)?[\w\[\]]+)\s*?\(')
DIG_TEMP = """
/* Example */
digraph {
    /* Note: HTML labels do not work in IE, which lacks support for <foreignObject> tags. */
    node [rx=5 ry=5 labelStyle="font: 300 14px 'Helvetica Neue', Helvetica"]
    edge [lineInterpolate=basis]

    A [labelType="html"
       label="A <span style='font-size:32px'>Big</span> <span style='color:red;'>HTML</span> Source!"];
    C;
    E [label="Bold Red Sink" style="fill: #f77; font-weight: bold"];
    A -> B [style="stroke: blue; stroke-width: 2px;" lineInterpolate=basis arrowheadStyle="fill: blue" ];
    B -> C;
    B -> D [label="A blue label" labelStyle="fill: #55f; font-weight: bold;"                 lineInterpolate=basis];
    D -> E [label="A thick red edge" style="stroke: #f77; stroke-width: 2px;" arrowheadStyle="fill: #f77"];
    C -> E;
    A -> D [labelType="html" label="A multi-rank <span style='color:blue;'>HTML</span> edge!"
                style="stroke: blue; stroke-width: 2px;"
                lineInterpolate=basis
                ];
   B -> E [lineInterpolate=basis]
}
"""

class NodeMeta(type):

    def __call__(cls, name, **kwargs):
        if name in Node.all_nodes:
            return Node.all_nodes[name]
        else:
            instance = super(NodeMeta, cls).__call__(name, **kwargs)
        return instance

    def __getitem__(self, k):
        if isinstance(k, str):
            node =  Node.all_nodes.get(k.strip(), Node(k.strip()))
            return node
        elif isinstance(k, Node):
            return k
        elif k is None:
            pass
        else:
            node = Node.all_nodes.get(str(k).strip(), Node(str(k).strip()))
            return node
    
    def __iter__(self):
        for m in Node.all_nodes:
            yield m

class Node(metaclass=NodeMeta):
    all_nodes = {}
    def __init__(self, name, val=""):
        self.name = name
        self.val = val
        self.next = []
        self.last = []
        self.label = name
        self.is_return = False
        self.__class__.all_nodes[name] = self
    
    def __add__(self, k):
        k_node = Node[k]
        if k_node not in self.next:
            self.next.append(k_node)
        if self not in k_node.last:
            k_node.last.append(self)
    
    def __repr__(self):
        return self.name
    
    def chains(self, links=[]):
        links.append(self)

        if not self.next:
            yield links
#        elif self in links and len(links) != 1:
#            yield links
        else:
            for next_node in self.next:
                k = links.copy()
                if next_node in k:
                    yield k
                    continue
                yield from next_node.chains(links=k)
    
    def _name(self, a):
        return re.sub(r'\W', '_' , str(a))
    
    @property
    def Name(self):
        return self._name(self)
    
    def graph(self, **kwargs):
        n = self.Name
        w = []
        if self.val:
            self.val = self.val.replace("\n", '</br>')
        
        if self.is_return:
            kwargs['style'] = "fill: #60b573; font-weight: bold"

        if 'label' not in kwargs:
            kwargs['label'] = str(self)
        if 'description' not in kwargs:
            kwargs['description'] = self.val
        for k,v in kwargs.items():
            w.append("%s=\"%s\"" %(k, v))
        res = n + " [" +  ",".join(w) + "];"
        return res
    
    @classmethod
    def tmp_graph(cls, obj,**kwargs):
        name = re.sub(r'\W', '_' , str(obj)) 
        if not 'label' in kwargs:
            kwargs['label'] = str(obj)
        w = []
        for k,v in kwargs.items():
            w.append("%s=\"%s\"" %(k, v))
        res = name + " [" +  ",".join(w) + "];" 
        nn = Node(str(obj), val=str(obj))
        if 'description' in kwargs:
            nn.val = kwargs.get('description')
        return name,res





class ChainMeta(type):
    chain_names = {}
    
    def __call__(cls, fr, to, **kwargs):
        name = str(fr).strip() + "-" + str(to).strip()
        if name in ChainMeta.chain_names:
            return ChainMeta.chain_names[name]
        else:
            instance = super(ChainMeta, cls).__call__(fr, to, **kwargs)
            ChainMeta.chain_names[name] = instance
        return instance

    def __iter__(self):
        for i in self.__class__.chain_names.values():
            yield i
    
    def __getitem__(self, key):
        res = []
        if isinstance(key, tuple):
            fr, to = key
            fr = Node[fr]
            to = Node[to]
            name = str(fr) +"-" + str(to)
            res.append(ChainMeta.chain_names.get(name))
        elif isinstance(key, str):
            
            fr = Node[key]
            for to in fr.next:
                name = str(fr) + "-" + str(to)
                res.append(ChainMeta.chain_names.get(name))
        return res

        

class Chain(metaclass=ChainMeta):
    
    def __init__(self, fr,to, val=None, attrs="", code='',num=None):
        fr = Node[fr]
        to = Node[to]
        self.fr = fr
        self.to = to
        self.val = val
        self.attrs = attrs
        self.num = num
        self.code = code
        fr + to

    @classmethod
    def clear(cls, fr=None,to=None):
        if fr or to:
            fr = Node[fr]
            to = Node[to]
            fr.next = []
            to.last = []
        else:
            ChainMeta.chain_names = {}
            for n in Node.all_nodes:
                node = Node[n]
                node.last = []
                node.next = []
    
    @classmethod
    def tables(cls, *node):
        var = []
        for i in node:
            for c in Node[i].chains([]):
                if c not in var:
                    var.append(c)
        var = list(map(list, itertools.zip_longest(*var)))
        values = tabulate.tabulate(var)
        print(values)
    
    def __add__(self, k):
        
        v = None
        k = str(k).strip()
        if "=" in k:
            k,v = k.split("=", 1)
        
        k = Node[k]
        # k connect to chains' last  
        self.to + k 
        c = Chain(self.to, k, val=v)
        return c
    
    def __repr__(self):
        if self.val:
            return "%s--|%s|-->%s" % (self.fr, self.val, self.to)
        else:
            return "%s-->%s" % (self.fr,  self.to)
    
    def _name(self, a):
        return re.sub(r'\W', '_' , str(a))

    def chains(self, links=[]):
        links.append(self.fr)

        if not self.to.next:
            links.append(self.to)
            yield links
        elif self.to in links:
            links.append(self.to)
            yield links
        else:
            for next_node in self.to.next:
                ch = Chain(self.to, next_node)
                k = links.copy()
                yield from ch.chains(links=k)
    
    def graph(self):
        condition = self.attrs.get("condition")
        connection = self.attrs.get("connection") 
        description = self.attrs.get("description")
        code = self.attrs.get("code")
        is_return = self.attrs.get("is_return")
        
        nodes = []
        chains = []
        
        for n in [self.fr, self.to]:
            if n.val:
                tn = n.graph(style="stroke: #f77")
                nodes.append(tn)
                if n == self.fr:continue
                try:
                    last_name = None
                    nnum = self.num
                    for l in n.val.split("</br>"):
                        
                        name , attrs = l.split("=", 1)
                        opers = OPERATOR.findall(attrs)
                        if opers:
                            if is_return:
                                node_name, node_str = Node.tmp_graph(opers[0],label=str(opers[0]) , labelStyle="fill: #fff font-weight:bold", description=l, code=code)
                            else:
                                node_name, node_str = Node.tmp_graph(opers[0],label=str(opers[0]) , labelStyle="fill: #fff font-weight:bold", style="fill: #f77; font-weight: bold", description=l, code=code)
                            nodes.append(node_str)
                        else:
                            if is_return:
                                node_name, node_str = Node.tmp_graph(attrs,label=str(attrs) , labelStyle="fill: #fff font-weight:bold", description=l, code=code) 
                            else:
                                node_name, node_str = Node.tmp_graph(attrs,label=str(attrs) , labelStyle="fill: #fff font-weight:bold", style="fill: #f77; font-weight: bold", description=l, code=code) 
                            nodes.append(node_str)

                        if last_name != None:
                            
                            # print(last_name, "--->", node_name)
                            # ch = last_name + " -> " +node_name
                            ch = node_name + " ->" +  n.Name

                        else:
                            ch = node_name + " ->" +  n.Name
                        chains.append(ch)
                        if node_name:
                            last_name = node_name
                        nnum +=1 
                        
                    #if last_name:
                    #    chains.append(last_name + " -> "+ n.Name)
                    cprint(n.val, 'green')
                except Exception as e:
                    cprint(e, 'red')
                    pass

        
        if is_return:
            n = self.to.graph(style="fill: #60b573; font-weight: bold")
            nodes.append(n)

        if connection:
            if str(connection).strip() in Node:
                _n = Node[str(connection).strip()]
                node_name = _n.Name
                node_str = _n.graph(label= _n.label  ,rx="10",ry="10",labelStyle="fill: #fff font-weight:bold", style="fill: #f77; font-weight: bold", code=code)
                # print("---- add ---", node_str)
            else:
                node_name, node_str = Node.tmp_graph(str(connection).strip(),label=str(connection).strip() , rx="10",ry="10",labelStyle="fill: #fff font-weight:bold", style="fill: #f77; font-weight: bold", description=description, code=code)
            nodes.append(node_str)
            if  condition:
                one = self.fr.Name +" -> " + node_name + " [label=\"%s\", style=\"stroke-dasharray: 5, 5; \" , color=\"gray\", description=\"%s\", code=\"%s\"];\n" % (condition, description, code)
                two = node_name + " -> " + self.to.Name + " [label=\"%s\", style=\"stroke-dasharray: 5, 5;\" , color=\"gray\", description=\"%s\", code=\"%s\"];" % (condition, description, code)
                chains.append(one)
                chains.append(two)
            else:
                chains.append(self.fr.Name +"-> "+ node_name + ";")
                chains.append(node_name + " ->" + self.to.Name + ";")
        else:
            if condition:
                res = self.fr.Name +"->" + self.to.Name + " [label=\"%s\", style=\"stroke-dasharray: 5, 5;\" , color=\"gray\", description=\"%s\"];" % (condition, description)
                chains.append(res)
            else:
                res =  self.fr.Name +"->" + self.to.Name + ";"
                chains.append(res)
        # print('no. ',self.num)
        return nodes + chains


    @classmethod
    def digraph(cls, *node_list, server=None):
        all_nodes = [Node[i] for i in node_list]
        li = []
        for node in all_nodes:
            for line in node.chains([]):
                for number, no in enumerate(line):
                    if number == 0:continue
                    c = cls(line[number-1], line[number])
                    if c not in li:
                        li.append(c)
        TEMP = """
/* Example */
digraph {
    /* Note: HTML labels do not work in IE, which lacks support for <foreignObject> tags. */
    node [rx=5 ry=5 labelStyle="font: 200 15px 'Helvetica Neue', Helvetica" style="stroke-dasharray: 5, 5;" , color="gray"]
    edge [lineInterpolate=basis]
    %s
    %s
}
"""
        
        same = []
        for n in node_list:
            tmp = n + " [label=\"%s\" rx=10 ry=10 labelStyle=\"fill: #fff\" style=\"fill: #22a6ff; font-weight: bold\" ];" % n
            same.append(tmp)

        content = {} 
        for i in li:
            for ggg in i.graph():
                if ggg not in content:
                    # print(ggg)
                    content[ggg] = i.num
        ccc = [i for i in sorted(content, key=lambda x: content[x])]
        TEMP = TEMP % ('\n    ' + '\n    '.join(same) , '\n    '.join(ccc))
        if server:
            requests.post(server, data={"graph":TEMP})
        return TEMP

# class Line:

#     def __init__(self, first_node):
#         self.first_node = Node[first_node]
#         self.all_chains = []
    
#     def chains(self):
#         link = []
#         f = self.first_node
#         for next_node in f.next:
#             ch = Chain(self.first_node, next_node)

def get_operator_from_line(line):
    words = FUNC.findall(line)
    opers = OPERATOR.findall(line)
    attrs = list(set(words) - set(opers))
    A = {}
    l = len(words)
    last_oper = None
    for i,w in enumerate(words):
        if i == l -1:
            if w not in opers:
                A[w] = last_oper
            else:
                last_oper = w
            break
        wp = None
        if w in opers:
            last_oper = w
            continue
        if i > 0:
            wp = words[i -1]
        if words[i+1] in opers:
            A[w] = words[i+1]
        if wp in opers:
            A[w] = wp
        else:
            A[w] = last_oper
    if not A and len(attrs) ==1 and len(opers) ==1:
        A[attrs[0]] = opers[0]
    if len(attrs) > 0 and len(opers) ==0:
        for ii in attrs:
            A[ii] = line
    return attrs, opers , A


def get_modules_info(Model, server='http://127.0.0.1:18080/'):
    init_code = inspect.getsource(Model.__init__)
    self_attr_dict = OrderedDict(INIT_ATT_EXE.findall(init_code))

    forward_code = [ i for i in inspect.getsourcelines(Model.forward)[0] if not i.strip().startswith("#") ]

    input_args = [WORD.findall(i)[0] for i in  INPUT_VARS.findall(forward_code[0])[0].split(",")[1:]]
    [Node(i) for i in input_args]
    
    levels = OrderedDict()
    levels_keys = []
    if_levels = OrderedDict()
    if_levels_keys = []
    for_levels = OrderedDict()
    for_levels_keys = []
    
    last_space_l = -1
    last = -1
    for_mask = 0
    if_mask = 0
    attrs = OrderedDict()

    [Node(input_arg) for input_arg in input_args]
    # if line endswith "," or "\" will compress lines to one line
    cache_lines = '' 
    output = []
    code = ''
    for num, line in enumerate(forward_code):
        if line.strip().endswith(","):
            
            if not cache_lines:
                cache_lines += line.rstrip()
            else:
                cache_lines += line.strip()
            
            continue
        
        elif cache_lines and line.strip().endswith(")"):
            
            cache_lines += line.strip()
            line = cache_lines
            cache_lines = ''
        
        code += line
        if not code.endswith("\n"):
            code += "\n"
        space = ST_SPANCE.findall(line)[0]
        space_l = len(space)
        line = line.strip()

        if not line:
            continue
        
        if num == 0:
            continue

        if line.startswith("#"):continue

        if_state = IF_STATE.findall(line)
        for_state = FOR_STATE.findall(line)
        attr_state = ATT.findall(line)
        _output = OUTPUT.findall(line)
        if _output:
            for i in ATTRS_NAME.findall(_output[0]):
                output.append(i)
            cprint(output, 'red')
        

        if space_l > last_space_l and last_space_l != -1:
            # cprint(line, 'red')
            levels[num-1] = space_l
            levels_keys.append(num-1)
            last = num
            
        # mask if state
        if if_state:
            if_levels[num] = space_l
            if_levels_keys.append(num)        
            if_mask += 1
        
        # mask for state
        elif for_state:
            for_levels[num] = space_l
            for_levels_keys.append(num)
            for_mask += 1



        # end space level
        if space_l < last_space_l:
            l = (last_space_l - space_l) // 4
            l_l =(last_space_l - space_l) %  4 
            if l_l != 0:
                l = 1
            # print("back len:", l, line)
            for i in range(l):
                n = levels_keys[-i -1]
                levels[n] = num

                # end for or if
                if n in for_levels_keys:
                    for_levels[n] = num
                    for_mask -= 1 
                if n in if_levels_keys:
                    if_levels[n] = num
                    if_mask -= 1 

        last_space_l = space_l

        
        if attr_state:
            # locate attr in attr_state from __init__ function
            tmp_dict = {}    
            attr_state = attr_state[0]
            n, v = attr_state
            funs_chains =[]
            chain = None

            words, opers, attr_oper = get_operator_from_line(attr_state[1])
            cprint("%s %s %s" %(words, opers,attr_oper), 'magenta')
            for k in FUNC.findall(attr_state[1]):
                

                if k in self_attr_dict:
                    w = self_attr_dict[k]
                else:
                    w = k
                
                if w in Node:
                    func = attr_oper.get(k)
                    if not func:
                        func = attr_state[1]
                    func = self_attr_dict.get(func, func)
                    # print("--> .  " ,func, k)


                    if w == n:
                        
                        _n = Node[n]
                        if _n.val:
                            _n.val += "\n" + line
                        else:
                            _n.val = line

                    chain = Chain(w, n, val=v,num=num, attrs={
                        "connection":func,
                        "description":line,
                        "code": code
                    })
                    # print( "{} = {}".format(n, v))
                funs_chains.append(w)
            if if_mask:
                if_str = forward_code[if_levels_keys[-if_mask]].strip()
                if chain:
                    # print("f ..")
                    chain.attrs['condition'] = if_str
            else:
                if_str = ''
            cprint(colored(n, 'yellow') + str(funs_chains) + colored(if_str, 'green'), 'green', attrs=['underline'] )
            tmp_dict[attr_state[0]] = funs_chains
            attrs[attr_state[0]] = tmp_dict[attr_state[0]]
        else:
            if output:
                
                _n  = Node[output[-1]]
                _n.is_return = True
                _n.val = line

    Chain.tables(*input_args)
    print(input_args)
    dig = Chain.digraph(*input_args, server=server)

            #print('[%3d]'%num, tmp_dict , ' in if: ', if_mask, ' in for:', for_mask)
    return if_levels, for_levels, levels,forward_code, self_attr_dict, dig