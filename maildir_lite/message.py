import hashlib, logging

# For standard Python message generation
import email, email.parser, email.policy

#For the message UID
import os, time, datetime, socket, random

log = logging.getLogger(__name__)

delivery_number = 0

class Message(object):
    subdir = "new"
    msgid = None
    info = None
    _content = None
    _content_hash = None
    mtime = 0
    last_stat = None
    _date = None
    
    def __init__(self, content=None, content_hash=None, subdir="new", msgid=None, info=None, mtime=0):
        if content:
            self.content = bytes(content)
        else:
            self.content = b""
        
        if content_hash:
            self._content_hash = content_hash
            
        if subdir:
            self.subdir = subdir
            
        if msgid:
            self.msgid = msgid
        else:
            self.msgid = self._gen_msgid()
            
        if info:
            self.info = info
            
        if mtime:
            self.mtime = mtime
            
    def __repr__(self):
        return "Message(msgid='%s', subdir='%s', info='%s', mtime='%s', content='%s')" % (self.msgid, self.subdir, self.info, self.mtime, self.content)
    
    def __bytes__(self):
        return bytes(self.content)
        
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
    def content(self):
        return bytes(self._content)
    
    @content.setter
    def content(self, newcontent):
        self._content = newcontent
        self._content_hash = None
        self._headers = None
        
    @property
    def content_hash(self):
        if not self._content_hash and self._content:
            self._content_hash = hashlib.sha256(self._content).hexdigest().encode("utf8")
        return self._content_hash
    
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
            if not self._date and self.headers and self.headers["Date"]:
                date_string = self.headers["Date"]
                self._date = email.utils.parsedate_to_datetime(date_string)
        except:
            pass #Lots of reasons for failures here, and nothing we care about.
                
        if not self._date and self.mtime:
            self._date = datetime.datetime.fromtimestamp(self.mtime)
        
        if not self._date:
            self._date = None #datetime.datetime.now()
        
        return self._date
            
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
