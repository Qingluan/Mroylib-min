#!/usr/bin/env python3
import json


class JsonFile:

    def __init__(self, f):
        with open(f) as fp:
            self.json = json.loads(fp.read())


class JsonData:

    def __init__(self):
        self.json = None


class TreeD:
    """
        the raw data must a dict and has 'id' and 'parent'
    """
    table = dict()
    table_keys = set()
    root = set()
    tmp_children = dict()
    tag = False

    def __init__(self, node):
        self.children = []
        self.id = node['id']
        self.parent_string = node['parent']
        self.parent = None
        self.attr = node
        self.load()

    def load(self):
        TreeD.tag = True
        if self.id in TreeD.table_keys:
            self = TreeD.get(self.id)
            return

        TreeD.table[self.id] = self
        TreeD.table_keys.add(self.id)

        # if node is root , add this to root
        if self.parent_string == self.id:
            TreeD.root.add(self)

        # collect children in tmp place
        if self.id in TreeD.tmp_children:
            self.children = TreeD.tmp_children.pop(self.id)

        # if this node's father exists ,
        # this node can collected by father directly
        if self.parent_string in TreeD.table_keys:
            TreeD.table[self.parent_string].child(self)
            self.parent = TreeD.table[self.parent_string]
        # if this node's father not exists now,
        # put node to a tmp place
        elif self.parent_string in TreeD.tmp_children:
            TreeD.tmp_children[self.parent_string].append(self)
        # if there is not any tmp place ...., created one
        else:
            TreeD.tmp_children[self.parent_string] = [self]

    def child(self, node):
        if node.id == self.id:
            return
        print(node.id, "--> ", self.id)
        self.children.append(node)

    def keys(self):
        return TreeD.table_keys

    def to_json(self):
        if self.children:
            c = {
                "id": self.id,
                "attr": self.attr,
                "children": [child.to_json() for child in self.children],
            }
            return c
        else:
            return {
                "id": self.id,
                "attr": self.attr,
                "size": len(self.id)
            }

    def __getitem__(self, k):
        return TreeD.get(k)

    def __call__(self):
        if not self.parent:
            return self
        else:
            return self.parent()

    def __str__(self):
        return self.id

    @staticmethod
    def tmp_to_root():
        for i in TreeD.tmp_children:
            t = TreeD({
                "id": i,
                "link": "http://" + i + "/",
                "parent": i,
                "title": i,
            })
            t.children = TreeD.tmp_children[i].copy()
            TreeD.root.add(t)
        TreeD.tmp_children = None

    @staticmethod
    def get_root():
        return TreeD.root

    @staticmethod
    def get(key):
        return TreeD.table[key]

    @staticmethod
    def clear():
        TreeD.table = dict()
        TreeD.table_keys = set()
        TreeD.root = set()
        TreeD.tmp_children = dict()


class Tree(JsonFile):

    def __init__(self, f):
        super(Tree, self).__init__(f)

        self.root = {
            "name": "root",
            "children": [],
        }
        self.__data = TreeD
        # if self.__data.tag:
        #     self.__data.clear()
        self.__parse()

    def __parse(self):
        for x in self.json:
            TreeD(x)

        self.__data.tmp_to_root()

    def __getitem__(self, k):
        return self.__data.table[k]

    def keys(self):
        return self.__data.table_keys

    def __call__(self):
        return self.__data.root

    def to_json(self, file=None):
        c = [subroot.to_json() for subroot in self()]
        self.root["children"] = c
        self.size = len(c)
        if not file:
            return self.root
        with open(file, "w") as fp:
            fp.write(json.dumps(self.root))
