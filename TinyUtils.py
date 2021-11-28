"""
A utility-module, that provides various features for working with TinyDB.\n
It makes it easier to work with TinyDB and adds mostly dynamic usage.\n
It uses a special technique called "virtual ID & table", which basically means, 
that it stores ID and table for every document in that specific document.
"""
## This is a script, that provides various utilities for working with tinydb.
## It uses a technique called "virtual tables and id's", which is basically storing
## IDs and Tables in fields for every tinydb-document (doc1 has id1 -> doc1 gets extra field containing id1)
## Why? 'Cause I am wayyy to lazy to code it properly and it works very fine like this.
## Only problem: this can cause some little issues during cross-compatibilty when trying to use a db that has been manually created, so use this script.
## And it might be annoying sometimes, but this script only accepts databases, that contain virtual ids and virtual tables, 
## this is done for reliability reasons.
##
## Global logic using the error checker: put it in every function, specify its type, if no exception is raised the main code should run without any errors
##
## I know its shitty, ive done my best to make it work reliably, i need it in the future, this took me 3 weeks to make. - jogroe 07/2021


## tinydb>=4.4.0 required and a normal python interpreter
from typing import Any
import tinydb
from tinydb import where
from tinydb.operations import set, delete
import os.path
import os
import ast


## only needs 3 globals, nice!
database = None
database_path_set = bool(False)
database_path = str("")


## Classes were created to give a better overview than just smashing all funcions raw into the module.


## Class to do all the managing with filepaths, instances, etc. 
class db:
    """
    Class to handle filepaths & instances.
    """


    ## Set the path to the db. Why? 'Cause it's used very often later, and it would be annoying to specify it every time.
    def set(db_path) -> None:
        """
        Set the path to the database-file. Mandatory for all other functions.
        """
        global database, database_path_set, database_path
        _debug.error_checker("set_db", filepath = db_path)
        database = tinydb.TinyDB(db_path)
        database_path = str(db_path)
        database_path_set = True


    ## Return the db currently selected, may be useful sometimes.
    def get() -> str:
        """
        Get the path of the database currently selected.
        """
        global database_path
        _debug.error_checker("normal")
        return str(database_path)


    ## empty the selected database COMPLETELY, use at own risk!
    def empty() -> None:
        """
        Delete the whole content of the database.
        """
        global database
        _debug.error_checker("normal")
        database.drop_tables()
        database.insert({"id": -1, "temporary_tables": ""})


    ## delete the entire database FILE COMPLETELY, use at own risk!
    def delete() -> None:
        """
        Delete the file containing the database.
        """
        global database_path, database
        _debug.error_checker("normal")
        database.close()
        os.remove(database_path)


    def close() -> None:
        """
        Close the database.
        """
        global database, database_path, database_path_set
        _debug.error_checker("normal")
        database.close()
        database_path = ""
        database_path_set = False


    def create(name) -> None:
        """
        Create a new database.
        """
        file = open(name, "w")
        file.close()
        db.set(name)
        database.insert({"id": -1, "temporary_tables": ""})
        database.insert({"id": 1, "table": "table1", "key1": "value1"})
        db.close()




class tables:
    """
    Class to handle virtual tables.
    """


    ## Read all virtual tables from the db, and return them
    def get() -> list:
        """
        Get all virtual tables from the database.
        """
        global database
        tabless = []
        _debug.error_checker("normal")
        cont = database.all()
        for item in cont:
            if item["id"] > 0:
                if item["table"] not in tabless:
                    tabless.append(str(item["table"]))
        temps = _debug.temp.get()
        for item in temps:
            if item not in tabless:
                tabless.append(item)
        return tabless


    def new(name) -> None:
        """
        Add a new table to the database.
        """
        global database
        _debug.error_checker("normal")
        if name in tables.get() or name == "":
            return
        else:
            _debug.temp.add(name)
 

    def rename(name_old,name_new) -> None:
        """
        Change the name of a table.
        """
        global database
        _debug.error_checker("normal")
        if name_old not in tables.get():
            error = f"Given table '{name_old}' was not found"
            raise RuntimeError(error)
        if name_old == name_new:
            return
        elif name_new == "":
            return
        else:
            database.update({"table": name_new}, where("table") == name_old)


    def merge(table1, table2) -> None:
        """
        Merge two tables, where `table1` gets kept and `table2` gets overridden.
        """
        global database
        _debug.error_checker("normal")
        if table1 == table2:
            return
        if table1 not in tables.get():
            error = f"Given table '{table1}' was not found"
            raise RuntimeError(error)
        if table2 not in tables.get():
            error = f"Given table '{table2}' was not found"
            raise RuntimeError(error)
        cont = database.all()
        for item in cont:
            if item["id"] > 0:
                if item["table"] == table2:
                    doc_id = item["id"]
                    database.update({"table":table1}, where("id") == doc_id)


    def delete(name) -> None:
        """
        Delete a table from the database, all documents in that table get deleted too!
        """
        global database
        _debug.error_checker("normal")
        if name not in tables.get():
            error = f"Given table '{name}' was not found"
            raise RuntimeError(error)
        if _debug.temp.check(name) == True:
            _debug.temp.delete(name)
        tables_new = tables.get()
        cont = database.all()
        to_delete = []
        for item in cont:
            if item["id"] > 0:
                if item["table"] == name:
                    to_delete.append(item.doc_id)
        database.remove(doc_ids = to_delete)


    ## easier to delete all docs and put a new temporary tables doc
    def delete_all() -> None:
        """
        Delete all tables, which implies deleting all documents.
        """
        global database
        _debug.error_checker("normal")
        database.drop_tables()
        database.insert({"id": -1, "temporary_tables": ""})
        



class documents:
    """
    Class to handle documents.
    """


    def new(doc, table, desired_id = 0):
        """
        Add a new document to the database. Specify an ID if needed, otherwise the next one that is free will be assigned.\n
        """
        global database
        _debug.error_checker("normal")
        ## id-checker (check id if free, else take next free one)
        if desired_id > 0:
            if desired_id in documents.get.ids():
                error = f"ID {desired_id} is already in use"
                raise RuntimeError(error)
            elif desired_id not in documents.get.ids():
                free_id = desired_id
        elif desired_id == 0:
            ids = documents.get.ids()
            free_id = 1
            while free_id in ids:
                free_id += 1
        ## table-checker:
        if table not in tables.get():
            error = f"Table '{table}' was not found"
            raise RuntimeError(error)
        else:
            table_to_use = table
        ## insert the new document
        if type(doc) != dict:
            error = f"Expected 'dict' but got {type(doc)} instead"
            raise RuntimeError(error)
        keys = list(doc)
        dict_to_insert = {}
        dict_to_insert["id"] = int(free_id)
        dict_to_insert["table"] = str(table_to_use)
        for key in keys:
            dict_to_insert[key] = doc[key]
        database.insert(dict_to_insert)



    class field:


        def update(document_id, key_old, key_new, value_new) -> None:
            """
            Update a field of a document. You can update key and value at once, or seperately.
            """
            global database
            _debug.error_checker("normal")
            if document_id not in documents.get.ids():
                error = f"ID {document_id} was not found"
                raise RuntimeError(error)
            doc = database.search(where("id") == document_id)
            if key_old == key_new and value_new == doc[key_old]:
                return
            elif key_old == key_new and value_new != doc[key_old]:
                database.update(set(key_old, value_new), where("id") == document_id)
                return
            ## pseudo-changing: delete old, insert slightly different new, done
            elif key_old != key_new:
                database.update(delete(key_old), where("id") == document_id)
                database.update({key_new: value_new}, where("id") == document_id)
                return


        def add(document_id, key, value) -> None:
            """
            Add a field to a document.
            """
            global database
            _debug.error_checker("normal")
            if document_id not in documents.get.ids():
                error = f"ID {document_id} was not found"
                raise RuntimeError(error)
            else:
                database.update({key: value}, where("id") == document_id)


        def delete(document_id, key) -> None:
            """
            Delete a field of a document.
            """
            global database
            _debug.error_checker("normal")
            if document_id not in documents.get.ids():
                error = f"ID {document_id} was not found"
                raise RuntimeError(error)
            else:
                database.update(delete(key), where("id") == document_id)


    def delete(document_id) -> None:
        """
        Delete a document COMPLETELY.
        """
        global database
        _debug.error_checker("normal")
        if document_id not in documents.get.ids():
            error = error = f"ID {document_id} was not found"
            raise RuntimeError(error)
        else:
            doc = database.get(where("id") == document_id)
            id_to_remove = doc.doc_id
            ids = []
            ids.append(int(id_to_remove))
            database.remove(doc_ids = ids)


    def change_id(id_old, id_new) -> str:
        """
        Change the ID of a document.
        """
        global database
        _debug.error_checker("normal")
        if id_old not in documents.get.ids():
            error = f"Given old ID '{str(id_old)}' was not found"
            raise RuntimeError(error)
        elif id_new in documents.get.ids():
            error = f"Given new ID '{str(id_new)}' is already assigned"
            raise RuntimeError(error)
        else:
            if id_old == id_new:
                doc = database.search(where("id") == id_old)
                return str(doc)
            else:
                database.update({"id":int(id_new)}, where("id") == id_old)
                doc_changed = database.searcH(where("id") == id_new)
                return str(doc_changed)


    def change_table(document_id, table) -> str:
        """
        Change the table of a document.
        """
        _debug.error_checker("normal")
        global database
        if str(table) not in tables.get():
            error = f"Given table '{str(table)}' was not found"
            raise RuntimeError(error)
        if document_id not in documents.get.ids():
            error = f"Given ID '{str(document_id)}' was not found"
            raise RuntimeError(error)
        database.update({"table": str(table)}, where("id") == document_id)
        doc_changed = database.search(where("id") == document_id)
        return str(doc_changed)


    ## created, cause there would be too much functions flying "raw" around
    class get:
        """
        Get various stuff/documents.
        """
        def ids() -> "list[int, int]":
            """
            Get all ID's used in the database.
            """
            global database
            ids = []
            _debug.error_checker("normal")
            cont = database.all()
            for item in cont:
                if item["id"] not in ids and item["id"] > 0:
                    ids.append(item["id"])
            return ids


        def by_id(document_id) -> Any:
            """
            Get a document by its specified ID. (accepts list or int)\n
            Returns a string or list, based on input.
            """
            global database
            _debug.error_checker("normal")
            if isinstance(document_id, int):
                if document_id not in documents.get.ids():
                    error = f"Given ID '{str(document_id)}' was not found"
                    raise RuntimeError(error)
                else:
                    doc = database.search(where("id") == document_id)
                return doc
            elif isinstance(document_id, list):
                docs = []
                for item in document_id:
                    number = int(item)
                    if number not in documents.get.ids():
                        error = f"Given ID '{str(number)}' was not found"
                        raise RuntimeError(error)
                    else:
                        doc = database.search(where("id") == number)
                        docs.append(doc)
                return docs


        def by_table(table) -> list:
            """
            Get all documents attached to a special table (accepts str or list)
            """
            global database
            docs = []
            _debug.error_checker("normal")
            if isinstance(table, str):
                if table in tables.get():
                    cont = database.all()
                    for item in cont:
                        if item["id"] > 0:
                            if str(item["table"]) == table:
                                docs.append(item)
            elif isinstance(table, list):
                cont = database.all()
                for t in table:
                    if t in tables.get():
                        for item in cont:
                            if item["id"] > 0:
                                if str(item["table"]) == str(t):
                                    docs.append(item)
            return docs


        ## this method is a horrible solution, wayy more elaborate than it could be, but the method i was planning to use 
        ## is coded that shitty, that you can't use it dynamically, so here we are
        def by_field(field, value = "") -> list:
            """
            Get all documents by providing a field, and additionally a value. (accepts str or list)
            """
            _debug.error_checker("normal")
            global database
            if value == "":
                if isinstance(field, str):
                    cont = database.all()
                    docs = []
                    for item in cont:
                        document = ast.literal_eval(str(item))
                        try:
                            waste = document[field]
                            if str(document) not in docs:
                                docs.append(str(document))
                        except:
                            pass
                    return docs
                elif isinstance(field, list):
                    cont = database.all()
                    docs = []
                    for item_cont in cont:
                        document = ast.literal_eval(str(item_cont))
                        for item_field in field:
                            try:
                                waste = document[item_field]
                                if str(document) not in docs:
                                    docs.append(str(document))
                            except:
                                pass
                    return docs
            else:
                if isinstance(field, str):
                    doc = database.search(where(field) == value)
                    return doc
                elif isinstance(field, list):
                    if len(field) == len(value):
                        docs = []
                        ind = 0
                        for item in field:
                            docs.append(str(database.search(where(item) == value[ind])))
                            ind += 1
                        return docs
                    else:
                        error = f"Lists 'field' ({len(field )} items) and 'value' ({len(value)} items need to have the same length"
                        raise ValueError(error)




## buggy shit, be aware!
class _debug:
    """
    Various debugging tools/internals, use with care. Some functions can have the database specified directly, 
    might be useful for fast usage.
    """




    ## class to handle temporary tables
    class temp:
        """
        Handle temporary virtual tables (= virtual tables, that are unused).
        """


        def get() -> list:
            """
            Get all temporary tables.
            """
            global database
            _debug.error_checker("normal")
            cont = database.all()
            for item in cont:
                if item["id"] == -1:
                    temp = str(item["temporary_tables"])
                    if temp != "":
                        temps = temp.split(", ")
                        temps_list = []
                        for item in temps:
                            temps_list.append(item)
                    elif temp == "":
                        temps_list = []
                    return temps_list


        def check(name) -> bool:
            """
            Check if a table is just temporary.
            """
            global database
            _debug.error_checker("normal")
            temps = _debug.temp.get()
            if name in temps:
                return True
            else:
                return False


        ## no idea, why i had to code it that dumb, but its fast and works so nvm
        def add(name) -> None:
            """
            Create a new temporary table.
            """
            global database
            _debug.error_checker("normal")
            tables_old = _debug.temp.get()
            tables_old.append(name)
            tables_new = ""
            for item in tables_old:
                if tables_old[-1] != item:
                    tables_new = tables_new + str(item) + ", "
                else:
                    tables_new = tables_new + str(item)
            database.update({"temporary_tables": tables_new}, where("id") == -1)


        def delete(name) -> None:
            """
            Delete a temporary table.
            """
            global database
            _debug.error_checker("normal")
            if _debug.temp.check(name) == False:
                error = f"Given table '{name}' was not found"
                raise RuntimeError(error)
            else:
                tables_old = _debug.temp.get()
                waste = tables_old.remove(name)
                tables_new = ""
                for item in tables_old:
                    if tables_old[-1] != item:
                        tables_new = tables_new + str(item) + ", "
                    else:
                        tables_new = tables_new + str(item)
                database.update({"temporary_tables": tables_new}, where("id") == -1)
                pass




    ## check if a database is readable for the module, this fuctions raises errors, if the database is not readable
    def database_readable(db_path = "") -> "tuple[bool,bool,bool]":
        """
        Check if database contains virtual tables / or id's.
        Returns if the database is readable, contains tables, contains id's.\n
        (to be readable, a database either needs to contain nothing, or id's and tables)
        """
        global database, database_path, database_path_set
        is_readable = contains_tables = contains_ids = True

        if db_path == "":
            pass
        else:
            if os.path.isfile(db_path) == False:
                filename = os.path.split(db_path)[1]
                error = f"Given file '{filename}' was not found"
                raise FileNotFoundError(error)
            elif os.path.isfile(db_path) == True:
                name, ext = os.path.splitext(db_path)
                if ext == ".json":
                    database = tinydb.TinyDB(db_path)
                    database_path = str(db_path)
                    database_path_set = True
                else: 
                    error = f"Expected .json file, got {ext} file instead"
                    raise RuntimeError(error)
        cont = database.all()
        if cont == []:
            contains_tables = contains_ids = False
            is_readable = True
            return is_readable, contains_tables, contains_ids
        for item in cont:        
            try:
                waste = item["table"]
            except KeyError:
                contains_tables = False
            try:
                waste = item["id"]
            except KeyError: 
                contains_ids = False
        if contains_tables == False and contains_ids == False:
            is_readable = False
        return is_readable, contains_tables, contains_ids


    ## little masterpiece, a function that should catch / raise ALL errors that can occur. if this function doesnt raise an error, everything should work correctly.
    def error_checker(type_arg = "", **kwargs) -> None:
        """
        An error checker, that can be used in a variety of ways. Pass type (and eventually required arguments) to the function, to use it.\n
        `set_db` requires: `filepath`\n
        `normal` requires: nothing\n
        `check_type` requires: `var_to_check` `type_to_check`
        """
        global database, database_path, database_path_set
        types_args = ["set_db", "normal", "check_type"]
        if type_arg == "":
            error = "Expected 1 argument, got 0 instead"
            raise RuntimeError(error)
        if type_arg not in types_args:
            error = f"Unknown argument type: {type_arg}"
            raise ValueError(error)
        ## type: set-db (pretty complex, see comments below)
        elif type_arg == "set_db":
            ## step 1: check if all args were given correctly
            if "filepath" not in list(kwargs):
                keys = "'"
                for item in list(kwargs):
                    keys = keys + str(item)
                    if list(kwargs).index(item) != len(kwargs)-1:
                        keys = keys + ", "
                keys = keys + "'"
                if len(kwargs) == 0:
                    keys = "no argument"
                error = f"Expected 'filepath' argument, got {keys} instead"
                raise ValueError(error)
            filepath = kwargs["filepath"]
            if filepath == "":
                error = "Given 'filepath' argument is empty"
                raise ValueError(error)
            ## step 2: check file metadata (right file type, existing etc)
            elif os.path.isfile(filepath) == False:
                filename = os.path.split(filepath)[1]
                error = f"Given file '{filename}' was not found"
                raise FileNotFoundError(error)
            elif os.path.isfile(filepath) == True and filepath != "":
                name, ext = os.path.splitext(filepath)
                if ext != ".json":
                    error = f"Expected .json file, got {ext} file instead"
                    raise RuntimeError(error)
                ## step 3: check if the database is usable for this module
                else: 
                    ## little critical part, cause file-content might not be suitable for tinydb, but fuck it
                    database = tinydb.TinyDB(filepath)
                    contains_tables = contains_ids = is_empty = True
                    cont = database.all()
                    if cont == []:
                        contains_tables = contains_ids = False
                        is_empty = True
                    elif cont != []:
                        is_empty = False
                        for item in cont:
                            if item["id"] > 0:        
                                try:
                                    waste = item["table"]
                                except KeyError:
                                    contains_tables = False
                                try:
                                    waste = item["id"]
                                except KeyError: 
                                    contains_ids = False
                    if is_empty == False:
                        if contains_tables == False:
                            error = "The given database is not readable, because it doesn't contain any virtual tables"
                            raise RuntimeError(error)
                        if contains_ids == False:
                            error = "The given database is not readable, because it doesn't contain any virtual id's"
                            raise RuntimeError(error)
                    database.close()
        ## type: check_type (check if a given var is a specific type)
        elif type_arg == "check_type":
            ## step 1: check if all args were given correctly
            if "var_to_check" and "type_to_check" not in list(kwargs):
                keys = "'"
                for item in list(kwargs):
                    keys = keys + str(item)
                    if list(kwargs).index(item) != len(kwargs)-1:
                        keys = keys + ", "
                keys = keys + "'"
                if len(kwargs) == 0:
                    keys = "no argument"
                error = f"Expected 'var_to_check' and 'type_to_check' argument, got {keys} instead"
                raise ValueError(error)
            var_to_check = kwargs["var_to_check"]
            type_to_check = kwargs["type_to_check"]
            if  var_to_check == "":
                error = "Given 'var_to_check' argument is empty"
                raise ValueError(error)
            if  type_to_check == "":
                error = "Given 'type_to_check' argument is empty"
                raise ValueError(error)
            ## step 2: convert "<type 'list'>" to "list" 
            type_unreadable = str(type(var_to_check))
            type_list  = type_unreadable.split("'")
            type_readable = type_list[1]
            ## stap 3: compare
            if str(type_readable) != str(type):
                error = f"Expected {str(type_to_check)}, got {str(type_readable)} instead"
                raise ValueError(error)
            elif str(type_readable) == str(type_to_check):
                return
        ## ADD FUTURE KEYWORDS HERE ##
        ## type: normal (just check if a database is set, or not)
        if type_arg == "normal":
            if len(kwargs) > 0:
                error = f"Expected 1 argument, got {int(len(kwargs)) + 1} arguments instead"
                raise RuntimeError(error)
            if database_path_set == False:
                error = "No db is set, use db.set() to set the path"
                raise RuntimeError(error)
            elif database_path_set == True:
                return