from os import path
import sys
from yaml import load, load_all

scriptroot = path.split(path.realpath(__file__))[0]
configroot = path.join(scriptroot, "../config")

def lazyloadconf(name):
    def loadconf():
        return load(open(path.join(configroot, "{}.yml".format(name)), "r"))
    
    return loadconf

fluentdconf = lazyloadconf("fluentd")()
redisconf = lazyloadconf("redis")()
secretconf = lazyloadconf("secret")()

if __name__ == "__main__":
    print(fluentdconf())
