import datetime
import logging
import os
import time
from .message import Message


log = logging.getLogger(__name__)

XATTR_MD5SUM = b"user.md5sum"
XATTR_DATE = b"user.date"

try:
    import xattr
    has_xattr = True
    log.debug("XATTR support enabled")
except:
    has_xattr = False
    log.debug("XATTR support unavailable: pyxattr not found")


class InvalidMaildirError(Exception):
    pass


# alias for mailbox compatability
class NoSuchMailboxError(InvalidMaildirError):
    pass


class Maildir(object):
    _parent = None
    _use_xattrs = False
    _last_update = 0
    _keys = {}
    
    path = None
    paths = []
    fs_layout = False
    
    # Turn on lazy updates if you do not expect this maildir to be
    # externally-modified while open (even by other Maildir instances!).
    lazy = False
    lazy_period = 5 #seconds to cache the directory list
    
    folder_seperator = "."
    
    def __init__(self, path, create=False, lazy=False, xattr=False, fs_layout=False):
        self.path = os.path.abspath(os.path.expanduser(path))
        self.lazy = lazy
        self.fs_layout = fs_layout
        if fs_layout == True:
            self.folder_seperator = "/"
        self.paths = {
            "cur": os.path.join(self.path, "cur"),
            "new": os.path.join(self.path, "new"),
            "tmp": os.path.join(self.path, "tmp")
        }
        
        # Does this path exist?
        if os.path.exists(self.path):
            # Good, now is it a directory?
            if not os.path.isdir(self.path):
                raise InvalidMaildirError("%s: not a directory" % self.path)
            # And is it a maildir?
            if not os.path.isdir( os.path.join(path, "cur") ):
                # Okay, can we fix it later?
                if not create:
                    raise InvalidMaildirError("%s: directory missing maildir properties" % self.path)
        
        # It doesn't exist, so can we create it?
        elif create is False:
            raise InvalidMaildirError("%s: path not found" % self.path)
        
        # Either it exists or we should create it. This solves both.
        if create:
            for subdir in self.paths.values():
                os.makedirs(subdir, mode=0o700, exist_ok=True)
        
        # See if we're a subfolder
        parent_path = os.path.dirname(self.path)
        try:
            self._parent = Maildir(parent_path, fs_layout=fs_layout)
        except InvalidMaildirError as e:
            self._parent = None
        
        # Turn on XATTRs, if we can/should.
        if xattr is True:
            _use_xattrs = has_xattr
        else:
            _use_xattrs = False
        
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
                    for dirent in os.scandir(subdir):
                        if dirent.name[0] == '.': continue
                        key = dirent.name.split(":")[0]
                        if dirent.is_file():
                            self._keys[key] = dirent.path
        
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
        
    def _path_for_message(self, message):
        filename = message.msgid
        if message.subdir is "cur" or message.flags:
            filename += ":" + (message.info or "2,")
        msg_path = os.path.join(self.path, message.subdir, filename)
        return msg_path
    
    def enumerate_messages(self, load_content=True):
        for subdir in self.paths.values():
            if os.path.isdir(subdir):
                for dirent in os.scandir(subdir):
                    msg = self._message_at_path(dirent.path, load_content=load_content)
                    yield msg
        return None

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
            if not msg.msg_md5 and self._use_xattrs and load_content:
                try:
                    xattrs = xattr.listxattr(path)
                    # logging.debug(xattrs)
                    if XATTR_MD5SUM in xattrs:
                        msg.msg_md5 = xattr.getxattr(path, XATTR_MD5SUM)
                        # logging.debug("Read md5: %s", msg.msg_md5)
                    else:
                        c = msg.content_hash
                        if c:
                            # logging.debug("Setting shasum xattr: %r", c)
                            xattr.setxattr(path, XATTR_MD5SUM, c)
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
                    self._use_xattrs = False
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
            if self._use_xattrs and msg.content_hash:
                xattr.setxattr(msg_path, XATTR_MD5SUM, msg.content_hash)
        except IOError:
            # read-only FS, unsupported on FS, etc.
            self._use_xattrs = False
            log.debug("host filesystem for %s does not support xattrs; disabling" % self.name)
        
        times = (msg.mtime, msg.mtime)
        os.utime(msg_path, times)
    
            
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
        return (self._parent != None)
        
    @property
    def name(self):
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
        return self.add(content=msg.content, msgid=msg.msgid, subdir=msg.subdir, info=msg.info, mtime=msg.mtime, content_hash=msg.content_hash)
    
    def add(self, content, msgid=None, subdir=None, info=None, mtime=None, content_hash=None):
        if not mtime:
            mtime = time.time()
        
        msg = Message(content=content, content_hash=content_hash, subdir="tmp", msgid=msgid, info=info, mtime=mtime)
                
        # Ensure we have a unique ID, as much as possible.
        while msg.msgid in self.keys():
            msg.msgid = msg._gen_msgid()
        
        # Write to tmp and update metadata
        self._write_message(msg)
        self._keys[msg.msgid] = self._path_for_message(msg)
        
        # Now that it's written out, move it to the proper destination.
        if subdir:
            msg.subdir = subdir
        elif msg.flags:
            msg.subdir = "cur"
        else:
            msg.subdir = "new"
        
        return self.update(msg.msgid, msg)
    
    def update(self, key, msg):
        """
        Updates a message's ID and/or content.
        """
        old_path = self._path_for_key(key)
        old_stat = os.stat(old_path)
        
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
    
    def _path_to_vpath(self, path):
        """
        Converts a filesystem path to a virtual path.
        """
        base, name = os.path.split(path)
        name = name.replace(self.folder_seperator, "/")
        return name
    
    def _vpath_to_path(self, vpath):
        """
        Converts a virtual path to a filesystem path.
        """
        # Get the parent Maildir's path
        mailbox_path = self.path
        if self.is_subfolder:
            mailbox_path = self._parent.path
        
        # Replace invalid FS characters with the seperator.
        name = vpath
        name = name.replace("/", self.folder_seperator)
        name = name.replace(":", self.folder_seperator)
        
        # Never allow a leading slash. Usually associated with
        # fs_layout, we never want this in the result anyway.
        if name[0] == '/':
            name = name[1:]
        
        # All Maildir++ directory names must start with a period.
        if self.fs_layout is False and name[0] != ".":
            name = "." + name
        
        path = os.path.join(mailbox_path, name)
        return path
        
    def list_folders(self):
        """
        Returns a list of child folder vpaths.
        """
        folders = [self.name]
        folder_root = self.path
        
        if self.is_subfolder:
            logging.debug("_ %s is a subfolder" % self.path)
            maildir_path = self._parent.path
            if self.fs_layout is False:
                folder_root = self.path + "."
        else:
            maildir_path = self.path
        
        for dirent in os.scandir(maildir_path):
            path = dirent.path
            logging.debug("inspecting %s" % path)
            # To be strict, one could check for dirent.name[0] == '.', but
            # doing it this way handles Dovecot FS-style Maildirs.
            if path.startswith(folder_root) and dirent.is_dir() and os.path.isdir( os.path.join(path,"cur") ):
                folders.append(self._path_to_vpath(path))
        
        return folders
    
    def get_folder(self, vpath):
        """
        Returns a new Maildir object for the given folder vpath.
        """
        if vpath == None or vpath == "" or vpath == "/": return self
        
        path = self._vpath_to_path(vpath)
        try:
            m = Maildir(path, create=False, xattr=self._use_xattrs, lazy=self.lazy, fs_layout=self.fs_layout)
            m.lazy_period = self.lazy_period
            return m
        except:
            raise NoSuchMailboxError(vpath)
        
    def create_folder(self, vpath):
        """
        Gets or creates a Maildir for the given vpath.
        """
        try:
            folder = self.get_folder(vpath)
            return folder
            
        except NoSuchMailboxError:
            path = self._vpath_to_path(vpath)
            folder = Maildir(path, create=True, xattr=self._use_xattrs, lazy=self.lazy, fs_layout=self.fs_layout)
            folder.lazy_period = self.lazy_period
            return folder
