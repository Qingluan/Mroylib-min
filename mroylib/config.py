import os
from configparser import ConfigParser

class Config:

    def __init__(self, file=None, name=None):
        self.file = file
        if not file and name:
            self.file = os.path.join(os.path.expanduser("~/.config/"), name)
            if not os.path.exists(os.path.expanduser("~/.config")):
                os.mkdir(os.path.expanduser("~/.config"))
        
        if not os.path.exists(self.file):
            raise FileNotFoundError("file : {file}".format(self.file))

        self.conf = ConfigParser()
        self.section = "DEFAULT"
        self.conf.read(self.file)
        self.sections = self.conf.sections()
        if len(self.sections) > 0:
            self.section = self.sections[0]

    def get(self, key, default=None):
        if key in self.keys:
            return self[key]
        else:
            return default

    def __getitem__(self, key):
        v = self.conf.get(self.section , key)        
        if v.startswith("~"):
            return os.path.expanduser(v)
        return v

    def __setitem__(self, key,val):
        if self.section not in self.conf.sections():
            self.conf.add_section(self.section)
        self.conf[self.section][key] = val

    def save(self):
        with open(self.file +".bak", 'w') as fp:
            self.conf.write(fp)
        os.rename(self.file +".bak", self.file)

    @property
    def keys(self):
        return self.conf.options(self.section)


    def to_dict(self):
        return {k:self.__getitem__(k) for k in  self.keys}