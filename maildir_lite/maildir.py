import os, logging
from .message import Message


log = logging.getLogger(__name__)

try:
    import xattr
    has_xattr = True
    log.debug("XATTR support enabled")
except:
    has_xattr = False
    log.debug("XATTR support unavailable: pyxattr not found")


XATTR_SHASUM = b"user.shasum"
XATTR_DATE = b"user.date"


class InvalidMaildirError(Exception):
    pass


# alias for mailbox compatability
class NoSuchMailboxError(InvalidMaildirError):
    pass


class Maildir(object):
    path = None
    paths = []
    _supports_xattr = has_xattr
    
    # Turn on lazy updates if you do not expect this maildir to be
    # externally-modified while open (even by other Maildir instances!).
    lazy = False
    lazy_period = 5 #seconds to cache the directory list
    
    _last_update = 0
    _keys = {}
    
    folder_seperator = "."
    
    def __init__(self, path, create=False, lazy=False):
        self.path = os.path.abspath(os.path.expanduser(path))
        self.lazy = lazy
        self.paths = {
            "cur": os.path.join(self.path, "cur"),
            "new": os.path.join(self.path, "new"),
            "tmp": os.path.join(self.path, "tmp")
        }
        
        # Does this path exist?
        if os.path.exists(self.path):
            # Good, now is it a directory?
            if not os.path.isdir(self.path):
                raise InvalidMaildirError(self.path)
            # And is it a maildir?
            if not os.path.isdir( os.path.join(path,"cur") ):
                raise InvalidMaildirError(self.path)
        
        # It doesn't exist, so can we create it?
        elif create is False:
            raise InvalidMaildirError(self.path)
        
        # Either it exists or we should create it. This solves both.
        if create:
            for subdir in self.paths.values():
                os.makedirs(subdir, mode=0o700, exist_ok=True)
        
    def __getitem__(self, key):
        return self.get_message(key)
        
    def __setitem__(self, key, message):
        self.update(key, message)
        
    def __delitem__(self, key):
        self.remove(key)
        
    def __iter__(self):
        for key in self.keys():
            msg = self[key]
            yield msg
            
    def __contains__(self, key):
        try:
            value = self[key]
            return True
        except KeyError:
            return False
        
    def __len__(self):
        return len(self.keys())
        
    def _refresh_msgs(self):
        update = False
        
        if not self._keys:
            update = True
        else:
            if self.lazy:
                if (self._last_update + self.lazy_period) > time.time():
                    return
            
            for subdir in self.paths.values():
                if os.path.getmtime(subdir) > (self._last_update + 2):
                    update = True
                    break;
        
        # Update keys
        if update:
            # log.debug("Refreshing message list for %r (%s; %d, %d)" % (self, self.name, self._last_update, len(self._keys)))
            
            self._last_update = time.time()
            self._keys = {}
            
            for subdir in self.paths.values():
                if os.path.isdir(subdir):
                    for filename in os.listdir(subdir):
                        key = filename.split(":")[0]
                        msgpath = os.path.join(self.path, subdir, filename)
                        if not filename[0] == '.' and os.path.isfile(msgpath):
                            self._keys[key] = msgpath
    
    def _path_for_key(self, key):
        # First try to fetch the key without triggering a potentially expensive refresh.
        try:
            path = self._keys[key]
            # Ensure the file exists and we don't have stale data.
            if os.path.exists(path):
                return path
        except KeyError:
            pass
            
        self._refresh_msgs()
        path = self._keys[key]
        return path
    
    def move_message(self, key, maildir):
        src_path = self._keys[key]
        filename = os.path.basename(src_path)
        dst_path = os.path.join(maildir.path, "new")
        dst_path = os.path.join(dst_path, filename)
        os.rename(src_path, dst_path)
        
        del self._keys[key]
        
        if self.lazy:
            self._last_update = time.time()
        else:
            self._last_update = 0
        
    def _path_for_message(self, message):
        filename = message.msgid
        if message.subdir is "cur" or message.flags:
            filename += ":" + (message.info or "2,")
        msg_path = os.path.join(self.path, message.subdir, filename)
        return msg_path
        
    def _message_at_path(self, path, load_content=True):
        try:
            content = None
            if load_content:
                f = open(path, "rb")
                content = f.read()
                f.close()
            
            mtime = os.path.getmtime(path)
            
            directory, filename = os.path.split(path)
            directory, subdir = os.path.split(directory)
            
            msgid = None
            info = None
            parts = filename.split(":")
            if len(parts) > 0:
                msgid = parts[0]
            if len(parts) > 1:
                info = parts[1]
            
            msg = Message(content=content, msgid=msgid, info=info, subdir=subdir, mtime=mtime)
            msg.last_stat = os.stat(path)
            if self._supports_xattr and load_content:
                try:
                    xattrs = xattr.listxattr(path)
                    # logging.debug(xattrs)
                    if XATTR_SHASUM in xattrs:
                        msg._content_hash = xattr.getxattr(path, XATTR_SHASUM)
                        # logging.debug("Read shasum: %s", msg._content_hash)
                    else:
                        c = msg.content_hash
                        if c:
                            # logging.debug("Setting shasum xattr: %r", c)
                            xattr.setxattr(path, XATTR_SHASUM, c)
                        else:
                            logging.warning("Could not generate content hash of %s", msgid)
                    
                    if XATTR_DATE in xattrs:
                        msg._date = xattr.getxattr(path, XATTR_DATE).decode("utf8")
                        msg._date = datetime.datetime.fromtimestamp(float(msg._date))
                        # logging.debug("Read date: %s", msg._date)
                    else:
                        d = str(msg.date.timestamp()).encode("utf8")
                        if d:
                            # logging.debug("Setting date xattr: %r", d)
                            xattr.setxattr(path, XATTR_DATE, d)
                        else:
                            logging.warning("Could not determine message date of %s", msgid)
                
                except IOError:
                    # read-only FS, unsupported on FS, etc.
                    self._supports_xattr = False
                    log.debug("host filesystem for %s does not support xattrs; disabling" % self.name)
            
            return msg
            
        except OSError:
            raise KeyError
    
    def _write_message(self, msg):
        msg_path = self._path_for_message(msg)
        
        f = open(msg_path, "wb")
        f.write(msg.content)
        f.close
        
        try:
            if self._supports_xattr and msg.content_hash:
                xattr.setxattr(msg_path, XATTR_SHASUM, msg.content_hash)
        except IOError:
            # read-only FS, unsupported on FS, etc.
            self._supports_xattr = False
            log.debug("host filesystem for %s does not support xattrs; disabling" % self.name)
        
        times = (msg.mtime, msg.mtime)
        os.utime(msg_path, times)
        
        msg.last_stat = os.stat(msg_path)
        
    def get_message(self, key, load_content=True):
        msg_path = self._path_for_key(key)
        msg = self._message_at_path(msg_path, load_content=load_content)
        if msg.subdir == "new":
            msg.subdir = "cur"
            self.update(key, msg)
            msg = self[msg.msgid]
        
        return msg
        
    @property
    def is_subfolder(self):
        if os.path.isdir( os.path.join( os.path.dirname(self.path), "cur" ) ):
            return True
        return False
        #return (os.path.basename(self.path)[0] == ".")
        
    @property
    def name(self):
        # name = os.path.basename(self.path)
        if self.is_subfolder:
            name = os.path.basename(self.path)
        else:
            name = self.folder_seperator
        name = name.replace(self.folder_seperator, "/")
        return name
    
    def keys(self):
        self._refresh_msgs()
        return self._keys.keys()
    
    def add_message(self, msg):
        return self.add(content=msg.content, msgid=msg.msgid, info=msg.info, mtime=msg.mtime, subdir=msg.subdir, content_hash=msg.content_hash)
    
    def add(self, content, msgid=None, subdir=None, info=None, mtime=None, content_hash=None):
        if not mtime:
            mtime = time.time()
        
        msg = Message(content=content, msgid=msgid, info=info, subdir="tmp", mtime=mtime)
        msg._content_hash = content_hash
        
        # Ensure we have a unique ID, as much as possible.
        while msg.msgid in self.keys():
            msg.msgid = msg._gen_msgid()
        
        # Write to tmp and update metadata
        self._write_message(msg)
        
        self._keys[msg.msgid] = self._path_for_message(msg)
        if self.lazy:
            self._last_update = time.time()
        else:
            self._last_update = 0
        
        msg.last_stat = os.stat(self._keys[msg.msgid])
        
        # Now that it's written out, move it to the proper destination.
        if subdir:
            msg.subdir = subdir
        elif msg.flags:
            msg.subdir = "cur"
        else:
            msg.subdir = "new"
        
        return self.update(msg.msgid, msg)
    
    def update(self, key, msg):
        old_path = self._path_for_key(key)
        
        # See if we have to rename it
        new_path = self._path_for_message(msg)
        if old_path and old_path != new_path:
            os.rename(old_path, new_path)
            self._keys[key] = new_path
            
            if self.lazy:
                self._last_update = time.time()
            else:
                self._last_update = 0
        
        # Verify the content
        old_stat = msg.last_stat
        new_stat = os.stat(new_path)
        if not (old_stat and old_stat == new_stat):
            log.debug("Checking message content (%r, %r)", old_stat, new_stat)
            old_msg = self._message_at_path(new_path)
            if old_msg.content != msg.content:
                self._write_message(msg)
            else:
                # Check the file's mtime
                if new_stat.st_mtime != msg.mtime:
                    times = (msg.mtime, msg.mtime)
                    os.utime(new_path, times)
        
        return msg.msgid
    
    def remove(self, key):
        os.remove(self._path_for_key(key))
        del self._keys[key]
        
        if self.lazy:
            self._last_update = time.time()
        else:
            self._last_update = 0
    
    def _path_to_folder(self, path):
        base, name = os.path.split(path)
        name = name.replace(self.folder_seperator, "/")
        return name
    
    def _folder_to_path(self, name):
        # print("In:", name)
        name = name.replace("/", self.folder_seperator)
        name = name.replace(":", self.folder_seperator)
        
        mailbox_path = self.path
        if self.is_subfolder: mailbox_path = os.path.dirname(mailbox_path)
        
        if name[0] != ".": name = "." + name
            
        path = os.path.join(mailbox_path, name)
        
        # print("Out:", path)
        return path
        
    def _folder_to_path_old(self, name):
        name = name.replace("/", self.folder_seperator)
        name = name.replace(":", self.folder_seperator)
        
        if self.is_subfolder:
            if name[0] != self.folder_seperator: name = self.folder_seperator + name
            parent_path = os.path.dirname(self.path)
            parent_name = os.path.basename(self.path)
            if not name.startswith(parent_name):
                name = parent_name + name
                
            path = os.path.join(parent_path, name)
        else:
            if name[0] != ".": name = "." + name
            path = os.path.join(self.path, name)
            
        return path
        
    def list_folders(self):
        folders = [self.name]
        
        if self.is_subfolder:
            maildir_path = os.path.dirname(self.path)
            folder_root = self.path + "."
        else:
            maildir_path = self.path
            folder_root = self.path
        
        for dirent in os.listdir(maildir_path):
            path = os.path.join(maildir_path, dirent)
            # To be strict, one could check for dirent[0] == '.', but
            # doing it this way handles Dovecot FS-style Maildirs.
            if path.startswith(folder_root) and os.path.isdir(path) and os.path.isdir( os.path.join(path,"cur") ):
                folders.append(self._path_to_folder(path))
        
        # folders.append(self._path_to_folder(self.path))
        
        return folders
    
    def get_folder(self, name):
        if name == None or name == "" or name == "/": return self
        
        path = self._folder_to_path(name)
        try:
            m = Maildir(path, create=False, lazy=self.lazy)
            m.lazy_period = self.lazy_period
            return m
        except:
            raise NoSuchMailboxError(name)
        
    def create_folder(self, name):
        try:
            folder = self.get_folder(name)
            return folder
            
        except NoSuchMailboxError:
            path = self._folder_to_path(name)
            folder = Maildir(path, create=True, lazy=self.lazy)
            folder.lazy_period = self.lazy_period
            return folder
