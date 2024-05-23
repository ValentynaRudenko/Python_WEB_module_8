from abc import ABC, abstractmethod
from task_get import search_by_name, search_by_tag, search_by_tags


class UserView(ABC):
    @abstractmethod
    def display_message(self, message):
        pass

    @abstractmethod
    def get_user_input(self):
        pass


class ConsoleView(UserView):
    def display_message(self, message):
        print(message)

    def get_user_input(self):
        return input("Enter a command: ")


def input_error(func):
    def inner(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except ValueError:
            return "Give me a command: value."
        except IndexError:
            return "Give me a command and a value."
        except KeyError:
            return "This command isn't in the command list"
    return inner


def parse_input(user_input):
    cmd, *args = user_input.split(": ")
    cmd = cmd.strip().lower()
    if len(args) > 0:
        args = args[0].split(",")
    return cmd, args


def main(view):
    view.display_message("Welcome to the assistant bot!")
    while True:
        user_input = view.get_user_input()
        if user_input.strip():
            command, args = parse_input(user_input)
            if command in ["close", "exit"]:
                view.display_message("Good bye!")
                break
            elif command == "hello":
                print("How can I help you? ")
            elif command == "name":
                print(search_by_name(args))
            elif command == "tag":
                print(search_by_tag(args))
            elif command == "tags":
                print(search_by_tags(args))
            else:
                print("Invalid command.")
        else:
            print("Your input is empty")


if __name__ == "__main__":
    console_view = ConsoleView()
    main(console_view)
