import os
import dropbox
import pandas as pd
from tabulate import tabulate 
from get_data import(create_pgconn,
                     get_uploaded_ids,
                     return_count_files)

class Command(object):
    def __init__(self):
        self.commands = { 
            "status" : self.status,
            "help" : self.help
        }

    def handle_command(self, user, command):
        response = "<@" + user + ">: "

        if command in self.commands:
            response += self.commands[command]()
        else:
           response += "Sorry I don't understand the command: " + command + ". " + self.help()

        return response

    def status(self):
        engine = create_pgconn('db_credentials.yaml')
        status_dropbox_updates = pd.read_sql('''
                                             select * 
                                             from dropbox_updates
                                             ''',
                                             con=engine
                                            )

        message = tabulate(status_dropbox_updates,
                           headers = 'keys',
                           tablefmt='fancy_grid')

        return message

    def help(self):
        response = "Currently I support the following commands:\r\n"

        for command in self.commands:
            response += command + "\r\n"
 
        return response
