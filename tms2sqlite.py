#!/usr/bin/env python3
import sqlite3
import argparse
import os
import binascii
import re
import time

argparser = argparse.ArgumentParser()
argparser.add_argument('-f', dest='format', metavar='FORMAT', type=str, help='input format (tms/htms, default: tms)', default='tms')
argparser.add_argument('-z', dest='zoom', metavar='ZOOM', type=int, help='only specified zoom', default=None)
argparser.add_argument('input_dir', metavar='INPUT', type=str, help='input directory')
argparser.add_argument('output_dir', metavar='TEMPLATE', type=str, help='output directory')
args = argparser.parse_args()

# ключ sqlite-файла
skey = lambda z,x,y: "%d:%d:%d" % (z,x>>8,y>>8)
# каталог расположения sqlite-файла
sdir = lambda d,z,x,y: os.path.join(d,"z%d"%(z+1),str(x>>10),str(y>>10))
# полное имя sqlite-файла
sname = lambda d,z,x,y: os.path.join(sdir(d,z,x,y),"%d.%d.sqlitedb" % (x>>8,y>>8))
# функция извлечения int из буфера
bint = lambda b,offset: int.from_bytes(b[offset:offset+4], byteorder='little', signed=False)

# max writers (opened sqlite databases at one time)
max_writers = 128
# max inserts between commits
#max_inserts = 1024
max_inserts = 0 # unlimited
debug = 0

# currently global counter for all writers
inserts_count = 0

class CacheWriter:
  def __init__(self, output_dir):
    self.output_dir = output_dir
    # writers, dict key = skey
    self.writers = {}
    # writers fifo list, values = skey
    self.writers_list = []

  def get_writer(self, z, x, y):
    k = skey(z, x, y)
    if debug>1: print ("cw:get_writer z=%d x=%d y=%d => k=%s" % (z,x,y,k))
    try:
      return self.writers[k]
    except KeyError:
      pass
    # no writer with that key so we need create it
    if len(self.writers_list) >= max_writers:
      # max_writers limit achieved, remove oldest
      r = self.writers_list.pop(0)
      if debug>1: print ("cw:remove writer k=%s" % r)
      w = self.writers[r]
      w.commit()
      w.close()
      self.writers[r] = None
    try:
      tdir = sdir(self.output_dir,z,x,y)
      if debug>1: print("cw:makedirs % s" % tdir)
      os.makedirs(tdir)
    except FileExistsError:
      pass
    sqldb = sname(self.output_dir, z, x, y)
    if debug>1: print ("cw:open_sqlite %s " % sqldb)
    w = sqlite3.connect(sqldb)
    self.writers[k] = w
    self.writers_list.append(k)
    cc = w.cursor()
    cc.execute("SELECT * FROM sqlite_master where tbl_name='t'")
    if len(cc.fetchall()) < 1:
      w.execute("CREATE TABLE t (x INTEGER NOT NULL,y INTEGER NOT NULL,v INTEGER DEFAULT 0 NOT NULL,c TEXT,s INTEGER DEFAULT 0 NOT NULL,h INTEGER DEFAULT 0 NOT NULL,d INTEGER NOT NULL,b BLOB,constraint PK_TB primary key (x,y,v))")
      w.execute("CREATE INDEX t_v_idx on t (v)")
    return w

  def store_tile(self, z, x, y, ft, blob):
    global inserts_count
    if debug>1: print ("cw:store_tile z=%d x=%d y=%d blob=(size=%d)" % (z,x,y,len(blob)))
    if args.format in ['tms','htms']:
      w = self.get_writer(z, x, y)
      #print (blob)
      try:
        w.execute("INSERT OR REPLACE INTO t (x,y,s,h,d,b) VALUES (?,?,?,?,?,?)", (x,y,len(blob),binascii.crc32(blob),int(time.time()),blob))
        inserts_count = inserts_count+1
        if max_inserts>0 and inserts_count >= max_inserts:
          self.flush()
          inserts_count = 0
      except AttributeError:
        print ("Something wrong with insert to sqlite file!")
        print ("x=%d y=%d s=%d d=%d" % (x,y,len(blob),int(time.time())))
        print ("crc32=%d" % binascii.crc32(blob))
        print ("blob[0:256]=%s" % blob[0:256])
        raise
    elif args.format == 'meta':
      magic = blob[0:4].decode("utf-8")
      assert magic == "META"
      count = bint(blob,4)
      mx = bint(blob,8)
      my = bint(blob,12)
      mz = bint(blob,16)
      assert mx == x
      assert my == y
      assert mz == z
      w = self.get_writer(z, mx, my)
      for i in range(0,count):
        offset = bint(blob,20+8*i)
        size = bint(blob,24+8*i)
        tx = mx+int(i/8)
        ty = my+(i%8)
        if debug>1: print ("%2d: offset=%d size=%d x=%d y=%d" % (i, offset, size, tx, ty))
        b = blob[offset:offset+size]
        try:
          w.execute("INSERT OR REPLACE INTO t (x,y,s,h,d,b) VALUES (?,?,?,?,?,?)", (tx,ty,len(b),binascii.crc32(b),int(time.time()),b))
          inserts_count = inserts_count+1
          if max_inserts > 0 and inserts_count >= max_inserts:
            self.flush()
            inserts_count = 0
        except AttributeError:
          print ("Something wrong with insert to sqlite file!")
          print ("x=%d y=%d s=%d d=%d" % (tx,ty,len(b),int(time.time())))
          print ("crc32=%d" % binascii.crc32(b))
          print ("blob[0:256]=%s" % b[0:256])
          raise
    else:
      raise BaseException("OOPS")

  def flush(self):
    for k in self.writers_list:
      w = self.writers[k]
      w.commit()

  def close(self):
    for k in self.writers_list:
      w = self.writers[k]
      w.close()

class Converter:
  def __init__(self,output_dir,format):
    self.output_dir = output_dir
    self.format = format
    self.cw = CacheWriter(output_dir)

  def detect_tile(self, filename):
    regex_tms = '.*/(\d+)/(\d+)/(\d+)\.(\w+)'
    regex_htms = '.*/(\d+)/(\d+)/(\d+)/(\d+)/(\d+)/(\d+)\.(\w+)'

    if self.format == 'tms':
      m = re.match(regex_tms, filename)
      if not m:
        print ("SKIP %s" % filename)
        return None
      z = m.group(1)
      x = m.group(2)
      y = m.group(3)
      ft = m.group(4)
      return int(z),int(x),int(y),ft
    elif self.format in ['htms','meta']:
      m = re.match(regex_htms, filename)
      if not m:
        print ("SKIP %s" % filename)
        return None
      z = m.group(1)
      x = 0
      y = 0
      for i in range(2,7):
        x = x<<4
        y = y<<4
        h = int(m.group(i))
        x = x | ((h&0xf0)>>4)
        y = y | (h&0x0f)
      ft = m.group(7)
      return int(z),int(x),int(y),ft;
    else:
      raise BaseException("OOPS")

  def convert(self, filename):
    t = self.detect_tile(filename)
    try:
      z, x, y, ft = t
    except TypeError:
      print (" OOPS TypeError %s" % filename)
      return
    print ("PROCESS %s" % filename)
    if args.format == 'meta' and ft != 'meta':
      raise BaseException("non-metafile found! try to use '-f meta' instead of '-f htms'")
    if args.format == 'htms' and ft == 'meta':
      raise BaseException("metafile found! try use '-f htms' instead of '-f meta'")
    if args.format in ['tms','htms','meta']:
      with open(filename, "rb") as f:
        blob = f.read()
      self.cw.store_tile(z,x,y,ft,blob)
    elif args.format == 'meta':
      print ("FIXME metafile support not implemented yet :(")
    else:
      raise BaseException("OOPS")

  def flush(self):
    self.cw.flush()

  def close(self):
    self.cw.close()

def dir_iterate(d, depth, converter):
  if depth == 0:
    if os.path.isfile(d):
      converter.convert(d)
    else:
      print (" is not file %s" % d)
  else:
    if not os.path.isdir(d):
      print (" is not dir  %s" % d)
      return
    for r in os.listdir(d):
      dd = os.path.join(d,r)
      dir_iterate(dd,depth-1,converter)

converter = Converter(args.output_dir, args.format)

if args.format == 'tms':
  depth = 3
elif args.format in ['htms','meta']:
  depth = 6
else:
  raise

if args.zoom is not None:
  args.input_dir = os.path.join(args.input_dir, str(args.zoom))
  depth = depth-1

dir_iterate(args.input_dir, depth, converter)

print ("Finalize...")
converter.flush()
converter.close()

print ("All done!")
