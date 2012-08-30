#!/usr/bin/python

# This code was written to solve the Thumbtack coding challenge up at http://www.thumbtack.com/challenges.
# Written by Jeremiah Lee, no warranty, yadda yadda.
#
# The idea behind the solution is that we will have a Context which stores a stack of Frame objects.
# Each Frame object has hashes to store the key/value pairs of the database as well as a separate
# hash for the number of each of the values currently being stored.
#
# The set, get, and unset operations occur on the current frame when they are performed. numequalto is a
# special case because for that count to be correct, it is needed to examine the frames lower in the stack.
# There is code below to handle that, but that is the reason that each of the Frame objects has a reference
# to the overall context.
#
# The transactional part of the database is actually fairly simple; when we start a transaction, we just overlay
# a new Frame by pushing it onto the stack of Frame objects that the context is keeping. When we roll it back, we
# pop the stack. Committing is a little more intense, causing use to walk the stack (all but the bottom Frame), applying
# any changes that have not already been applied from "higher" Frames.
#
# The main pain in this code is the numequalto operation because it needs to know whether a given key was used lower on
# the stack. This pain is encapsulated in the decrease method. I chose not to pass the context directly to that method
# because I did not want the API to have a 1 call take the context but not the rest of them. That is why I made it part
# of the Frame construction.
#
# One note about running this code, you can either feed it a file via STDIN to see the results or run the script
# and it will be interactive; as you type commands, you will see results. This is on purpose. :-D

import sys
import re

class Frame:

    def __init__(self, context):
        self.data = {}
        self.numbers = {}
        self.deleted = set()
        self.context = context

    # increase is just a helper to keep the numequalto data correct.
    def increase(self, var_name, value):
        self.numbers[ value ] = self.numbers.get(value, 0) + 1

    # decrease is a helper to keep the numequalto data correct. It needs to look
    # lower in the stack to know whether the var_name was used there so that it
    # can decrease the count of the value associated with the var_name lower in
    # the stack.
    def decrease(self, var_name):
        if var_name in self.data:
            value = self.data[var_name]

            self.numbers[ value ] = self.numbers.get(value, 0) - 1

            return

        value = self.context.get(var_name, False)

        if value is not None:
            self.numbers[value] = self.numbers.get(value, 0) - 1

    def set(self, var_name, value):
        self.decrease(var_name)
        self.increase(var_name, value)
        self.deleted.discard( var_name )

        self.data[var_name] = value


    def get(self, var_name):
        if var_name in self.data:
            return self.data[var_name]
        else:
            return None

    def unset(self, var_name):
        self.decrease(var_name)
        self.deleted.add(var_name)

        if var_name in self.data:
            del(self.data[var_name])

    def get_all_deleted(self):
        return list(self.deleted)

    def keys(self):
        return self.data.keys()

    def numequalto(self, value):
        return self.numbers[value] if value in self.numbers else 0

    def is_unset(self, var_name):
        return var_name in self.deleted

class Context:

    def __init__(self):
        self.frames = [Frame(self)]

    def current_frame(self):
        return self.frames[-1]

    def set(self, var_name, value):
        frame = self.current_frame()
        frame.set(var_name, value)

    def unset(self, var_name):
        self.current_frame().unset(var_name)

    def get(self, var_name, do_print=True):
        value = None

        for frame in reversed(self.frames):
            if frame.is_unset(var_name):
                value = 'NULL'
                break

            value = frame.get(var_name)

            if value is not None:
                break

        if do_print:
            print (value if value is not None else 'NULL')

        return value

    def numequalto(self, value):
        count = 0

        for frame in reversed(self.frames):
            count += frame.numequalto(value)

        print count

    def begin(self):
        self.frames.append( Frame(self) )

    def rollback(self):
        if len(self.frames) == 1:
            print "INVALID ROLLBACK"
        else:
            self.frames.pop()

    # basic methodology of the commit:
    # -walk the frames in reverse, excepting frames[0] (base_frame)
    # -for each of these, as long as the key name has not already been seen:
    #   -examine all of the unset variables first, unsetting them in the base_frame, marking them as seen
    #   -examine all of the keys in the current frame, setting them in the base_frame, marking them as seen
    # -remove the currently last frame from the stack of frames.

    def commit(self):
        if len(self.frames) == 1:
            print "INVALID COMMIT - NO TRANSACTION(S) IN PROGRESS"
            return

        seen = set()

        base_frame = self.frames[0]

        for frame in reversed(self.frames[1:]):
            for deleted in frame.get_all_deleted():
                if deleted in seen:
                    continue
                base_frame.unset(deleted)
                seen.add(deleted)

            for key in frame.keys():
                if key in seen:
                    continue
                
                base_frame.set(key, frame.get(key))
                seen.add(key)

            self.frames.pop()

    def call(self, args):
        func_name = args[0].lower()

        try:
            if len(args[1:]) > 0:
                getattr(Context, func_name)(self, *args[1:])
            else:
                getattr(Context, func_name)(self)
        except AttributeError:
            print "INVALID OPERATION (" + func_name + ") - Type 'END' to exit"

def main():
    context = Context()

    while 1:
        try:
            line = sys.stdin.readline()
            line = line.rstrip()

            if line == 'END':
                break

            context.call(re.split('\s+', line))
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    main()
