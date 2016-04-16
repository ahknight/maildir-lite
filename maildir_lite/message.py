import hashlib, logging

# For standard Python message generation
import email.utils, email.parser, email.policy

#For the message UID
import os, time, datetime, socket, random


log = logging.getLogger(__name__)
delivery_number = 0


class Message(object):
    _content = None
    _date = None
    subdir = "new"
    msg_id = None
    msg_md5 = None
    msg_size = 0
    msg_vsize = 0
    info = None
    mtime = 0
    
    def __init__(self, content=None, content_hash=None, subdir="new", msgid=None, info=None, mtime=0):
        if content:
            self.content = content
        else:
            self.content = b""
            
        if subdir:
            self.subdir = subdir
            
        if msgid:
            self.msgid = msgid
        else:
            self.msgid = self._gen_msgid()
        
        if content_hash:
            self.msg_md5 = content_hash
        
        if not self.msg_size:
            self.msg_size = len(self._content)
        
        if info:
            self.info = info
            
        if mtime:
            self.mtime = mtime
    
    def __repr__(self):
        return "Message(msgid='%s', subdir='%s', info='%s', mtime='%s', content='%s')" % (self.msgid, self.subdir, self.info, self.mtime, self.content)
    
    def __bytes__(self):
        return self.content
    
    def __str__(self):
        '''
        Don't trust this string to be encoded correctly for non-UTF8/ASCII messages.
        Use email.message if you want to actually use the message.
        '''
        return bytes(self).decode("utf8", errors="ignore")
    
    def __format__(self, formatspec):
        return str(self).__format__(formatspec)
    
    def _gen_msgid(self):
        global delivery_number
        now = datetime.datetime.now()
        
        seconds_number = time.time()
        hostname_string = socket.gethostname()
        random_number = random.getrandbits(32)
        microsecond_number = now.microsecond
        process_number = os.getpgid(0)
        delivery_number += 1
        
        msgid_str = "%d.R%dM%dP%dQ%d.%s" % (seconds_number, random_number, microsecond_number, process_number, delivery_number, hostname_string)
        return msgid_str
    
    @property
    def msgid(self):
        props = {}
        if self.msg_md5:
            props["MD5"] = self.msg_md5
        else:
            self.msg_md5 = self.content_hash
            props["MD5"] = self.msg_md5
        
        if self.msg_size:
            props["S"] = self.msg_size
        
        if self.msg_vsize:
            props["W"] = self.msg_vsize
        
        msgid = self.msg_id
        for k in sorted(list(props.keys())):
            msgid += ",%s=%s" % (k, props[k])
        return msgid
    
    @msgid.setter
    def msgid(self, msgid):
        props = msgid.split(",")
        self.msg_id = props.pop(0)
        for prop in props:
            try:
                k,v = prop.split("=")
                if k == "MD5":
                    self.msg_md5 = v
                elif k == "S":
                    self.msg_size = v
                elif k == "W":
                    self.msg_vsize = v
            except:
            	continue
    
    @property
    def content(self):
        return self._content
    
    @content.setter
    def content(self, newcontent):
        self._content = bytes(newcontent)
        self.msg_md5 = None
        self.msg_size = 0
        self.msg_vsize = 0
        self._headers = None
        
    @property
    def content_hash(self):
        if not self.msg_md5 and self._content:
            self.msg_md5 = hashlib.md5(self._content).hexdigest()
        return self.msg_md5
    
    @property
    def headers(self):
        if self._content and len(self._content):
            if not self._headers:
                parser = email.parser.BytesParser(policy=email.policy.default)
                msg = parser.parsebytes(self._content, headersonly=True)
                self._headers = msg
            return self._headers
        else:
            return None
    
    @property
    def date(self):
        if self._date:
            return self._date
        
        try:
            if self.headers and self.headers["Date"]:
                date_string = self.headers["Date"]
                self._date = email.utils.parsedate_to_datetime(date_string)
                return self._date
        except:
            pass #Lots of reasons for failures here, and nothing we care about.
        
        if self.mtime:
            self._date = datetime.datetime.fromtimestamp(self.mtime)
            return self._date
        
        return None #datetime.datetime.now()
    
    @property
    def flags(self):
        return self.info[2:] if self.info and self.info[0] is "2" and len(self.info) > 2 else ""
    
    @flags.setter
    def flags(self, newflags):
        self.info = "2," + newflags
    
    def add_flags(self, flags):
        flags = set(flags + self.flags)
        self.flags = "".join(sorted(flags))
    
    def remove_flags(self, flags):
        cflags = set(self.flags)
        cflags = cflags.difference( set( flags ) )
        self.flags = "".join(sorted(cflags))

"""
Compatability class for mailbox.MaildirMessage
"""
class MaildirMessage(Message):
    
    def __init__(self, message=None):
        super().__init__(message)
    
    def get_subdir(self):
        return self.subdir
    
    def set_subdir(self, new_value):
        if new_value in ["cur", "new"]:
            self.subdir = new_value
    
    def get_flags(self):
        return self.flags
    
    def set_flags(self, new_value):
        self.flags = new_value
    
    def add_flag(self, new_value):
        self.add_flags(new_value)
    
    def remove_flag(self, new_value):
        self.remove_flags(new_value)
    
    def get_date(self):
        return self.mtime
    
    def set_date(self, new_value):
        self.mtime = new_value
    
    def get_info(self):
        return self.info
    
    def set_info(self, new_value):
        self.info = new_value
