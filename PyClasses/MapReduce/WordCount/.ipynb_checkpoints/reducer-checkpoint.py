#!/usr/bin/env python3
import sys

def reducer():
    current_word = None
    current_count = 0

    for line in sys.stdin:
        word, count = line.strip().split('\t')
        count = int(count)

        if current_word == word:
            current_count += count
        else:
            if current_word is not None: 
                print(f"{current_word}\t{current_count}")
            current_word = word
            current_count = count

    # Emit the last word's count
    if current_word is not None:
        print(f"{current_word}\t{current_count}")

if __name__ == "__main__":
    reducer()