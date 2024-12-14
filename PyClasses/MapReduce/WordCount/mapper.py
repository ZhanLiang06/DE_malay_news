#!/usr/bin/env python3
import sys

def mapper():
    for line in sys.strip:
        word = line.strip()
        if word:  
            print(f"{word}\t1")

if __name__ == "__main__":
    mapper()