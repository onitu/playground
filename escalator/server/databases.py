import os.path

import plyvel


class Databases(object):
    class OpenError(Exception):
        pass

    class NotExistError(Exception):
        pass

    def __init__(self, working_dir='dbs'):
        self._databases = {}
        self._names = []
        self._working_dir = working_dir

    def __contains__(self, uid):
        return 0 <= uid < len(self._names)

    def get_db(self, name):
        return self._databases[name]

    def get_name(self, uid):
        if uid not in self:
            raise IndexError('Database with uid {} does not exist'.format(uid))
        return self._names[uid]

    def get(self, uid):
        return self.get_db(self.get_name(uid))

    def connect(self, name, create=False):
        try:
            name = os.path.join(self._working_dir, name)
            if name not in self._databases:
                self._databases[name] = plyvel.DB(name,
                                                  create_if_missing=create)
                self._names.append(name)
            return self._names.index(name)
        except plyvel._plyvel.IOError as e:
            raise Databases.OpenError(*e.args)
        except plyvel._plyvel.Error as e:
            raise Databases.NotExistError(*e.args)

    def list_dbs(self):
        return list(self._names)
