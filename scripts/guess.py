#!/usr/bin/env python
import random


def play_game(secret, attempts=5):
    tip = None
    print("Myslim si cislo od 1 do 20")
    pokusy = 0

    while tip != secret and pokusy < attempts:
        tip = int(input("Tvoj tip: "))
        pokusy += 1

        if tip < secret:
            print(f"Hmm... Moje cislo je vacsie ako {tip}.")

        elif tip > secret:
            print(f"Hmm... Moje cislo je mensie ako {tip}.")

        else:
            print(f"Super! Uhadol si. Moje cislo je {secret}.")

    if tip != secret:
        print("Nie! Minul si vsetky pokusy. Skus znova!")


playing = True

while playing == True:
    play_game(secret=random.randint(1, 20))

    choice = input("Chceš hrať znova? (a/n) ")
    if choice.upper() not in ("A", "Y", "ANO", "YES"):
        playing = False

print("Ďakujem, že si si zahral túto mega šupa bomba špica hru")
