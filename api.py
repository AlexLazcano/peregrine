import sys
import signal


def callFunc(command: str ):
    match command.lower():
        case "match":
            print("You want to match")
        case "count": 
            print("You want to count")
        case _: 
            print("Unknown operation")




if __name__ == "__main__":
    while True:
        try:
            task = input("Usage: command <arg1> <arg2> ... ").split()
            print(f"ARG 1 {task[0]}")
            print(f"ARG 2 {task[1]}")
            print(f"ARG 3 {task[2]}")
            callFunc(task[0])
        except IndexError:
            print("Invalid input")



