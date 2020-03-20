import os
import dropbox
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
        dbx = dropbox.Dropbox(os.environ['DROPBOX_API_KEY'])
        engine = create_pgconn('db_credentials.yaml')
        return_files = return_count_files(dropbox_conn = dbx,
                                          engine=engine,
                                          path='/noaa/integrated_surface_updated/clean')
        message = tabulate(return_files,
                           headers = 'keys',
                           tablefmt='fancy_grid')

        return message

    def help(self):
        response = "Currently I support the following commands:\r\n"

        for command in self.commands:
            response += command + "\r\n"
 
        return response
